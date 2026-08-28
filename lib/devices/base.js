'use strict';

const modbus = require('jsmodbus');
const net = require('net');
const HomeyEventEmitter = require('./homeyEventEmitter.js');
const util = require('../util.js');
const coalesce = require('../modbus/coalesce.js');
const decodeData = require('../modbus/decodeData.js');

class Base extends HomeyEventEmitter {

  modbusSettings = null;
  #deviceRegistryHandler = null;
  #intervalIds = [];
  #deviceType = null;
  #socket = null;
  client = null;
  #infoRegistriesRead = false;
  // Health check loop with exponential backoff reconnect logic
  #healthCheckInterval = null;
  #healthCheckRetries = 0;
  #maxHealthCheckDelay = 600_000; // 10 minutes maximum backoff
  // Guards so overlapping polls and socket resets don't run concurrently on the
  // shared client. A failing read cycle can take much longer than the polling
  // interval, so without these the interval would stack requests on the socket.
  #refreshing = false;
  #reconnecting = false;
  // A Base instance is single-use. Once its owning Homey device replaces or
  // destroys the session, asynchronous validation, reads and health checks must
  // not reconnect the old socket or emit data from an orphaned poll.
  #stopped = false;
  // Many inverters (SMA included) accept only a single Modbus TCP connection and
  // are slow to release it, so resetting the socket on every glitch churns that
  // one slot and makes things worse. We recover in place instead and only hard-
  // reset the socket after this many consecutive desynced sweeps, as a last resort.
  #consecutivePoisonedSweeps = 0;
  #maxPoisonedSweepsBeforeReset = 5;
  #socketSettleMs = 3000;
  // Ranges (keyed "start:count") that the device rejected as a single coalesced
  // read - e.g. the span crosses a register the device treats as illegal. Once
  // blocklisted we read that run one register at a time on every poll instead of
  // repeatedly attempting (and failing) the range read.
  #coalesceBlocklist = new Set();
  // Individual register ids currently failing to read, tracked so recovery can
  // be logged once when they start responding again.
  #failedRegisters = new Set();
  // Construction is intentionally non-blocking so callers can attach EventEmitter
  // listeners first. Device owners that need a usable long-lived session await
  // this promise instead of treating object construction as connection success.
  #readyPromise = null;
  #resolveReady = null;
  #rejectReady = null;
  #readySettled = false;

  constructor(deviceRegistryHandler, options = {}) {
    super();
    this.#deviceRegistryHandler = deviceRegistryHandler;
    this.options = options;
    this.#readyPromise = new Promise((resolve, reject) => {
      this.#resolveReady = resolve;
      this.#rejectReady = reject;
    });
    // Pairing/discovery consumers use events rather than waitUntilReady(). Keep
    // their rejected readiness promise from becoming an unhandled rejection.
    this.#readyPromise.catch(() => {});

    // Pairing/discovery sessions pass neither refreshInterval nor timeout, so log
    // 'unset' for those rather than 'undefined' / 'undefineds' - diagnostic logs
    // from users are read as-is and a stray 'undefineds' reads like a defect.
    const refreshIntervalLabel = this.options.refreshInterval ? `${this.options.refreshInterval}s` : 'unset';
    const timeoutSetting = Number(this.options.timeout);
    const timeoutLabel = (Number.isFinite(timeoutSetting) && timeoutSetting > 0) ? `${timeoutSetting}s` : 'unset';
    this._logMessage('INFO', `Initializing with options: host=${this.options.host}, port=${this.options.port}, refreshInterval=${refreshIntervalLabel}, timeout=${timeoutLabel} (effective ${this.#resolveTimeoutMs()}ms), autoClose=${this.options.autoClose}`);

    // Deferred by one tick on purpose. Callers (pairing, repair, discovery)
    // attach their 'properties'/'error' listeners immediately after construction,
    // and the synchronous part of an async function runs before the constructor
    // returns - so an error raised early in initialization would be emitted with
    // no listener attached. For 'error' that throws and takes the app down; for
    // any other event it is silently lost and the caller waits forever.
    this._setTimeout(() => {
      this.#initializeDevice();
    }, 0);
  }

  waitUntilReady() {
    return this.#readyPromise;
  }

  #markReady() {
    if (!this.#readySettled) {
      this.#readySettled = true;
      this.#resolveReady();
    }
  }

  #failInitialization(err) {
    if (!this.#readySettled) {
      this.#readySettled = true;
      const error = err instanceof Error ? err : new Error(util.formatError(err));
      this.#rejectReady(error);
    }
  }

  // Single funnel for every 'error' this session raises.
  //
  // EventEmitter treats 'error' specially: emitting it with no listener attached
  // throws, which in a Homey app means the whole app goes down. That is reachable
  // in normal operation - disconnect() detaches all listeners while a read or a
  // socket callback may still be in flight - so no error path may call
  // this.emit('error') directly.
  _emitError(err) {
    if (this.#stopped) {
      return;
    }
    if (this.listenerCount('error') === 0) {
      this._logMessage('INFO', `Unobserved error: ${util.formatError(err)}`);
      return;
    }
    this.emit('error', err);
  }

  async #initializeDevice() {
    if (this.#stopped) {
      this.#failInitialization(new Error('Session stopped before initialization'));
      return;
    }

    try {
      if (!this.options.host || !util.validateIPaddress(this.options.host)) {
        const error = new Error(`Invalid IP address '${this.options.host}'`);
        this._logMessage('INFO', error.message);
        this.#failInitialization(error);
        this._emitError(error);
        return;
      }

      // Long-lived device sessions use a lightweight Modbus probe before opening
      // their polling socket. An auto-close pairing/repair session must not do
      // that: its property read already validates identity, and a second rapid
      // connection can exhaust an SMA inverter's single Modbus TCP slot.
      if (!this.options.autoClose) {
        const available = await util.isModbusAvailable(this.options.host, this.options.port, this._logMessage.bind(this), this.#resolveTimeoutMs());
        // The owning Homey device may have replaced this session while its
        // asynchronous validator was still using the temporary socket.
        if (this.#stopped) {
          this.#failInitialization(new Error('Session stopped during Modbus validation'));
          return;
        }
        if (!available) {
          const error = new Error(`Connection failed: Modbus device at '${this.options.host}:${this.options.port}' did not respond`);
          this._logMessage('INFO', error.message);
          this.#failInitialization(error);
          this._emitError(error);
          return;
        }

        // The validator opened and dropped a Modbus session. Give the inverter
        // time to release its connection slot before opening the polling socket.
        await this._sleep(this.#socketSettleMs);
        if (this.#stopped) {
          this.#failInitialization(new Error('Session stopped before connecting'));
          return;
        }
      }

      // A session is ready only after its persistent socket connects. For an
      // auto-close session this call also completes the identity/property read
      // and closes the only socket used by the operation.
      try {
        await this.#initListenersAndConnect();
      } catch (err) {
        this._logMessage('DEBUG', `Initial connection attempt failed: ${util.formatError(err)}`);
        this.#failInitialization(err);
        return;
      }

      if (this.#stopped) {
        this.#failInitialization(new Error('Session stopped while connecting'));
        return;
      }
      this.#markReady();

    } catch (err) {
      this._logMessage('ERROR', `Validation failed: ${util.formatError(err)}`);
      this.#failInitialization(err);
      this._emitError(err);
    }
  }

  // Modbus request timeout in milliseconds. Derived from the per-device
  // 'timeout' setting (in seconds), falling back to 5s when unset or invalid.
  // Raising it helps inverters that respond slowly (responses arriving after
  // the default 5s otherwise time out and then desync the connection).
  #resolveTimeoutMs() {
    const seconds = Number(this.options.timeout);
    if (Number.isFinite(seconds) && seconds > 0) {
      return Math.round(seconds * 1000);
    }
    return 5000;
  }

  async #initListenersAndConnect() {
    if (this.#stopped) {
      return;
    }

    const socket = new net.Socket();
    this.#socket = socket;
    // Disable Nagle's algorithm. Modbus TCP is a chatty request/response
    // protocol: Nagle (on by default) can hold a small request waiting for the
    // previous ACK while the peer's delayed-ACK waits for data, stalling
    // back-to-back register reads. That is consistent with what we see on some
    // SMA inverters - a single read succeeds but a multi-register sweep times
    // out - and disabling it is the recommended default for Modbus TCP.
    socket.setNoDelay(true);
    // Bound the TCP connect. socket.connect() ignores an options.timeout, so
    // without this timer a peer that accepts the SYN into a black hole - which
    // SMA devices do while they still hold their single Modbus session for an
    // earlier client - leaves the connect hanging on the OS timeout (over two
    // minutes on Linux) with no event for the caller to act on. Cleared as soon
    // as we are connected so it can never fire later as an idle timeout on the
    // long-lived polling socket.
    socket.setTimeout(this.#resolveTimeoutMs());
    this.client = new modbus.client.TCP(socket, 3, this.#resolveTimeoutMs());

    return new Promise((resolve, reject) => {
      // Ensure the promise settles exactly once. Previously a connection error
      // only emitted 'error' and never settled the promise, so callers that
      // await this (health check / reconnect) could hang forever.
      let settled = false;

      socket.connect(this.options,
        async () => {
          settled = true;
          // Connected: drop the connect deadline. Polling is intentionally idle
          // between sweeps, so an inactivity timeout here would be a false alarm.
          socket.setTimeout(0);
          if (this.#stopped) {
            socket.destroy();
            resolve();
            return;
          }
          this._logMessage('INFO', `Client connected on IP '${this.options.host}'`);

          if (this.options.autoClose) {
            // Read properties and then disconnect. Use try/finally so the socket
            // is always closed even if reading properties fails (avoids leaks).
            try {
              await this.#readProperties();
            } finally {
              this._logMessage('INFO', 'Auto close is enabled, disconnecting!');
              this.disconnect();
            }
            resolve();
            return;
          }

          this.#initilializeTimers();

          this.#startHealthCheckLoop();

          resolve();
        });

      socket.on('error', (err) => {
        if (!this.#stopped) {
          this._emitError(err);
        }
        // Only reject for errors during the initial connect. Once connected,
        // socket errors are handled by the health check loop instead.
        if (!settled) {
          settled = true;
          if (this.#stopped) {
            resolve();
          } else {
            reject(err);
          }
        }
      });

      // Only reachable during the connect phase; the timer is cleared on connect.
      socket.on('timeout', () => {
        if (settled) {
          return;
        }
        settled = true;
        const err = new Error(`Timed out connecting to ${this.options.host}:${this.options.port}`);
        this._logMessage('WARN', err.message);
        socket.destroy();
        if (this.#stopped) {
          resolve();
        } else {
          this._emitError(err);
          reject(err);
        }
      });

      socket.on('close', () => {
        this._logMessage('INFO', `Client closed for IP '${this.options.host}'`);
        // Destroying a socket while it is still connecting is allowed to emit
        // only 'close'. Settle the pending connect so teardown cannot leave a
        // health-check callback awaiting forever.
        if (!settled) {
          settled = true;
          if (this.#stopped) {
            resolve();
          } else {
            reject(new Error('Modbus socket closed before connecting'));
          }
        }
      });
    });
  }

  // Tears down the current socket/client and establishes a fresh connection.
  // Used as a last resort to recover from a desynchronized Modbus stream (see
  // #refreshReadings): destroying the socket flushes any stale/late response
  // still in the OS buffer and the new jsmodbus client resets its transaction id
  // counter, so requests and responses line up again.
  async #reconnect() {
    if (this.#stopped || this.#reconnecting) {
      return;
    }
    this.#reconnecting = true;
    try {
      if (this.#socket) {
        this.#socket.removeAllListeners();
        this.#socket.destroy();
      }
      // Give the inverter time to release its (often single) Modbus connection
      // slot before reconnecting, otherwise the new connection can be refused or
      // silently ignored while the old one lingers.
      await this._sleep(this.#socketSettleMs);
      if (this.#stopped) {
        return;
      }
      await this.#initListenersAndConnect();
      this._logMessage('INFO', 'Modbus socket reset complete after out-of-sync recovery');
    } catch (err) {
      // Leave further recovery to the health check loop / device watchdog.
      this._logMessage('INFO', `Failed to reset Modbus socket: ${util.formatError(err)}`);
    } finally {
      this.#reconnecting = false;
    }
  }

  #initilializeTimers() {
    //If refresh interval is set, and we don't have timers
    //initialized already - then create them
    if (this.options.refreshInterval && this.#intervalIds.length === 0) {
      this._logMessage('INFO', 'Timers initialized');
      this.#intervalIds.push(this._setInterval(() => {
        this.#refreshReadings().catch((err) => {
          this._logMessage('INFO', `Unhandled error while refreshing readings: ${util.formatError(err)}`);
        });
      }, 1000 * this.options.refreshInterval));
    }
  }

  disconnect() {
    this.#stopped = true;
    // This Base instance is never reused. Detach its Homey-facing listeners so
    // late socket/read failures from an in-flight operation cannot affect the
    // replacement session.
    this.removeAllListeners();

    for (const timer of this.#intervalIds) {
      this._clearInterval(timer);
    }
    this.#intervalIds = [];

    if (this.#healthCheckInterval) {
      this._clearTimeout(this.#healthCheckInterval);
      this.#healthCheckInterval = null;
    }

    if (this.#socket) {
      this.#socket.destroy();
    }
  }

  #isConnected() {
    return this.client && this.client._socket && this.client._socket.readable && this.client._socket.writable && !this.client._socket.destroyed && !this.client._socket.connecting;
  }

  // Reads device type + info registries once and emits 'properties'.
  // Returns { ok, poisoned }: `ok` is true when properties were emitted;
  // `poisoned` is true when a read desynchronized the connection so the caller
  // can reset the socket. Never throws / rethrows: callers run in fire-and-forget
  // contexts (the polling timer and the socket 'connect' callback), and the raw
  // jsmodbus error is a plain object (UserRequestError, not an Error), so
  // rethrowing would surface as an unhandled '[object Object]' rejection.
  async #readProperties() {
    let poisoned = false;
    try {
      // Device type is critical - without it we can't map any registers, so a
      // failure here means we retry on the next poll rather than proceeding.
      let deviceTypeValues;
      try {
        const results = await Promise.all([
          this.client.readHoldingRegisters(util.DEVICE_TYPE_REGISTER, 2)
        ]);
        deviceTypeValues = results[0].response._body._valuesAsArray;
      } catch (err) {
        poisoned = util.isConnectionPoisoningError(err);
        this._emitError(new Error(`Failed to set Device Type! ${util.formatError(err)}`));
        return { ok: false, poisoned };
      }

      const decodedType = this.#deviceRegistryHandler.decodeDeviceType(deviceTypeValues);

      // A reachable inverter that is still booting or asleep in night mode
      // answers the device-type read with SMA's "information not available"
      // sentinel (0xFFFFFD) instead of its model id. Without the model we can't
      // pick a register map, so treat it exactly like a failed device-type read:
      // emit an error, do NOT read info registries, do NOT emit properties, and
      // do NOT mark info as read - just retry on the next poll. Otherwise a real
      // (sleeping) inverter gets pinned to an "UNKNOWN" type and the default
      // register map until the app restarts.
      if (decodeData.isDeviceNotReady(decodedType)) {
        this._emitError(new Error('Device type not available yet (device is booting or asleep); will retry on next poll'));
        return { ok: false, poisoned };
      }

      this.#deviceType = decodedType;
      this.modbusSettings = this.#deviceRegistryHandler.getModbusRegistrySettings(this.#deviceType);

      // Info registries are read tolerantly: a register that times out yields
      // zeros (the device applies sensible defaults) so a marginal inverter can
      // still finish initializing instead of being stuck forever.
      const { words, poisoned: batchPoisoned } = await this.#readRegistrySet(this.#deviceRegistryHandler.getInfoRegistries(this.modbusSettings), 'Info');
      poisoned = poisoned || batchPoisoned;

      // A timeout/out-of-sync result aborts the registry set. Do not decode or
      // publish incomplete properties; retry the complete set on a later poll.
      if (batchPoisoned || this.#stopped) {
        return { ok: false, poisoned };
      }

      const properties = this.#deviceRegistryHandler.getInfoValues(this.modbusSettings, words);
      properties.deviceType = this.#deviceType;
      this.emit('properties', properties);
      this.#infoRegistriesRead = true;
      return { ok: true, poisoned };

    } catch (err) {
      this._emitError(new Error(`Failed to set Device Type! ${util.formatError(err)}`));
      return { ok: false, poisoned };
    }
  }

  getDeviceCapabilities() {
    return this.#deviceRegistryHandler.getCapabilityKeys(this.modbusSettings);
  }

  // Reads a set of register descriptors, coalescing contiguous ones into single
  // range reads to cut round-trips (lower latency and desync risk on slow/busy
  // networks). Returns { words, poisoned } where `words` is a map of
  // { registerKey: number[] } consumed directly by the registry handler's keyed
  // decoders. `label` is used only in recovery logs ('Device' | 'Info').
  //
  // Resilience mirrors the single-register path:
  // - A coalesced read that fails because the connection went out of sync flags
  //   `poisoned`, zero-fills that run and aborts the registry set immediately.
  //   Continuing would put more requests behind a response that may arrive late.
  //   Sweep-level logic decides whether the idle gap is enough to drain it or a
  //   socket reset is required after repeated poisoned sweeps.
  // - A coalesced read the device rejects for another reason (e.g. the span
  //   crosses an illegal register) is blocklisted and falls back to per-register
  //   reads, so a single unmapped register can't blank the whole run.
  async #readRegistrySet(registries, label) {
    const words = {};
    let poisoned = false;
    const runs = coalesce.groupRegistersIntoRuns(registries);

    for (const run of runs) {
      const runKey = `${run.start}:${run.count}`;

      // A lone register, or a run previously rejected as a range, is read one
      // register at a time.
      if (run.registers.length === 1 || this.#coalesceBlocklist.has(runKey)) {
        const res = await this.#readRegistersIndividually(run.registers, label);
        Object.assign(words, res.words);
        poisoned = poisoned || res.poisoned;
        if (res.poisoned) {
          break;
        }
        continue;
      }

      try {
        const result = await this.client.readHoldingRegisters(run.start, run.count);
        const runWords = util.getRegisterWords(result);
        const slices = coalesce.sliceRunWords(runWords, run);
        for (const reg of run.registers) {
          words[reg.key] = slices[reg.key];
          if (this.#failedRegisters.delete(reg.registryId)) {
            this._logMessage('INFO', `${label} register ${reg.registryId} (${reg.comment}) recovered`);
          }
        }
      } catch (runErr) {
        this._logMessage('DEBUG', `${label} coalesced read ${run.start}+${run.count} failed: ${util.formatError(runErr)}`);

        if (util.isConnectionPoisoningError(runErr)) {
          // Stop immediately. jsmodbus advances its queue as soon as a request
          // times out, so issuing another read here lets the late response to
          // this request collide with that next request and cascades failures
          // through the rest of the sweep.
          poisoned = true;
          this._emitError(new Error(`Failed to read range ${run.start}+${run.count} for device type '${this.#deviceType}'`));
          for (const reg of run.registers) {
            words[reg.key] = new Array(reg.count).fill(0);
          }
          break;
        } else {
          // Range read unsupported/rejected - remember it so future polls skip
          // the range attempt, and fall back to per-register reads.
          if (!this.#coalesceBlocklist.has(runKey)) {
            this.#coalesceBlocklist.add(runKey);
            this._logMessage('INFO', `${label} coalesced read ${run.start}+${run.count} rejected, using per-register reads`);
          }
          const res = await this.#readRegistersIndividually(run.registers, label);
          Object.assign(words, res.words);
          poisoned = poisoned || res.poisoned;
          if (res.poisoned) {
            break;
          }
        }
      }
    }

    return { words, poisoned };
  }

  // Reads each register individually (the fallback / lone-register path).
  // Returns { words, poisoned }. Failed reads are zero-filled (so the sweep still
  // decodes) and tracked in #failedRegisters for one-shot recovery logging.
  async #readRegistersIndividually(registers, label) {
    const words = {};
    let poisoned = false;
    for (const reg of registers) {
      const reading = await util.modbusReading(
        this.client,
        { registryId: reg.registryId, registerCount: reg.count },
        this.#deviceType,
        this
      );
      words[reg.key] = reading.values;

      if (reading.poison) {
        // Do not issue another request on a stream that may still receive this
        // request's late response. The containing registry set is abandoned.
        poisoned = true;
        break;
      } else if (reading.failed) {
        if (!this.#failedRegisters.has(reg.registryId)) {
          this.#failedRegisters.add(reg.registryId);
        }
      } else if (this.#failedRegisters.delete(reg.registryId)) {
        this._logMessage('INFO', `${label} register ${reg.registryId} (${reg.comment}) recovered`);
      }
    }
    return { words, poisoned };
  }

  async #refreshReadings() {
    // Don't stack a new cycle on top of a still-running one or an in-progress
    // reconnect; both share the single Modbus client/socket. A failing sweep can
    // take much longer than the polling interval, so without this the interval
    // would pile requests onto the socket.
    if (this.#stopped || this.#refreshing || this.#reconnecting) {
      if (!this.#stopped) {
        this._logMessage('DEBUG', 'Skipping readings, a refresh or reconnect is already in progress');
      }
      return;
    }

    this.#refreshing = true;
    let poisoned = false;
    // if/else (no early return) so control always reaches the reconnect check.
    try {
      if (!this.#isConnected()) {
        this._logMessage('INFO', 'Skipping readings since socket is not connected!');
      } else if (!this.#infoRegistriesRead) {
        this._logMessage('INFO', 'Info registries not read yet, reading them now!');
        const result = await this.#readProperties();
        poisoned = result.poisoned;
      } else if (!this.modbusSettings) {
        this._logMessage('INFO', 'Modbus settings object is null!');
      } else {
        const { words, poisoned: readPoisoned } = await this.#readRegistrySet(this.#deviceRegistryHandler.getReadingRegistries(this.modbusSettings), 'Device');
        poisoned = readPoisoned;
        if (this.#stopped) {
          return;
        }
        if (readPoisoned) {
          // The set is incomplete and may contain zero-filled failed ranges.
          // Publishing it would overwrite valid capabilities and falsely tell
          // the availability watchdog that fresh data was received.
          this._logMessage('DEBUG', 'Discarding partial readings from poisoned Modbus sweep');
        } else {
          const readings = this.#deviceRegistryHandler.getReadingValues(this.modbusSettings, words);
          this.emit('readings', readings);
        }
      }
    } catch (err) {
      // Never let this fire-and-forget polling method reject; log readably.
      this._logMessage('INFO', `Failed to refresh readings: ${util.formatError(err)}`);
    } finally {
      this.#refreshing = false;
    }

    if (this.#stopped) {
      return;
    }

    // A read desynchronized the request/response stream (timeout / OutOfSync).
    // Rather than resetting the socket every time - which churns the inverter's
    // single Modbus connection slot and tends to make things worse - recover in
    // place: skip and let the idle gap before the next poll drain the stale
    // response. Only force a full socket reset after repeated back-to-back
    // failures, as a last resort. A clean sweep resets the counter.
    if (poisoned) {
      this.#consecutivePoisonedSweeps++;
      if (this.#consecutivePoisonedSweeps >= this.#maxPoisonedSweepsBeforeReset && !this.#reconnecting) {
        this._logMessage('INFO', `Modbus out of sync for ${this.#consecutivePoisonedSweeps} cycles, resetting socket as a last resort`);
        this.#consecutivePoisonedSweeps = 0;
        await this.#reconnect();
      } else {
        this._logMessage('INFO', 'Modbus connection out of sync, retrying on next poll without resetting');
      }
    } else {
      this.#consecutivePoisonedSweeps = 0;
    }
  }

  #startHealthCheckLoop() {
    if (this.#healthCheckInterval) {
      return;
    }

    const runCheck = async () => {
      if (this.#stopped) {
        return;
      }

      // A poison-triggered reconnect is already rebuilding the socket; don't
      // race it with a second reconnect from here.
      if (this.#reconnecting) {
        this.#healthCheckInterval = this._setTimeout(runCheck, 10_000);
        return;
      }

      if (this.#isConnected()) {
        this.#healthCheckRetries = 0;
        this.#healthCheckInterval = this._setTimeout(runCheck, 10_000); // Normal interval if healthy
        return;
      }

      try {
        this._logMessage('INFO', 'Health check failed, attempting to reconnect...');

        // Simple cleanup
        if (this.#socket) {
          this.#socket.removeAllListeners();
          this.#socket.destroy();
        }

        await this.#initListenersAndConnect();
        if (this.#stopped) {
          return;
        }
        this.#healthCheckRetries = 0;
      } catch (err) {
        if (this.#stopped) {
          return;
        }
        this.#healthCheckRetries++;
        const delay = Math.min(1000 * 2 ** this.#healthCheckRetries, this.#maxHealthCheckDelay);
        this._logMessage('INFO', `Reconnect failed, retrying in ${delay} ms`);
        this._clearTimeout(this.#healthCheckInterval);
        this.#healthCheckInterval = this._setTimeout(runCheck, delay);
        return;
      }

      if (!this.#stopped) {
        this.#healthCheckInterval = this._setTimeout(runCheck, 10_000); // Normal interval if reconnected
      }
    };
    runCheck();
  }
}
module.exports = Base;
