'use strict';

const net = require('net');
const modbus = require('jsmodbus');

// SMA device-type register (holding register, spans 2 words). Reading it serves
// as both the connectivity probe and the way we identify the model to select
// the correct register map.
const DEVICE_TYPE_REGISTER = 30053;
exports.DEVICE_TYPE_REGISTER = DEVICE_TYPE_REGISTER;

exports.pad = function (num, size) {
  return String(num).padStart(size, '0');
}

exports.validateIPaddress = function (ipaddress) {
  if (/^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(ipaddress)) {
    return (true)
  } else {
    return (false)
  }
}

exports.isModbusAvailable = function (host, port, logger = null, timeoutMs = 5000) {
  // Default logger function if none provided
  const log = logger || ((level, message) => console.log(message));

  return new Promise((resolve) => {
    const socket = new net.Socket();
    // Disable Nagle's algorithm - Modbus TCP is a chatty request/response
    // protocol where Nagle + delayed-ACK can stall back-to-back reads.
    socket.setNoDelay(true);
    const client = new modbus.client.TCP(socket, 3, timeoutMs);
    const options = { host, port };

    socket.setTimeout(timeoutMs);

    socket.on('connect', function () {
      log('INFO', `Validator: Connected to ${host}:${port}, testing Modbus communication...`);

      Promise.all([
        client.readHoldingRegisters(DEVICE_TYPE_REGISTER, 2)
      ]).then((results) => {
        log('INFO', `Validator: Modbus communication successful! Device responded with data.`);
        socket.destroy();
        resolve(true);
      }).catch((err) => {
        log('ERROR', `Validator: Modbus read failed: ${err.message}`);
        socket.destroy();
        resolve(false);
      });
    });

    socket.on('error', function (err) {
      log('ERROR', `Validator: Modbus connection to ${host}:${port} failed: ${err.message}`);
      resolve(false);
    });

    socket.on('timeout', function () {
      log('WARN', `Validator: Modbus connection to ${host}:${port} timed out`);
      socket.destroy();
      resolve(false);
    });

    socket.on('close', function () {
      log('DEBUG', `Validator: Connection to ${host}:${port} closed`);
    });

    log('INFO', `Validator: Attempting Modbus connection to ${host}:${port}...`);
    socket.connect(options);
  });
}

// Default upper bound for how long a pairing / discovery session may wait for a
// device to publish its properties. Generous enough for a slow inverter that
// needs several Modbus round-trips, but bounded so the UI can always report a
// result. See waitForProperties().
const PROPERTIES_TIMEOUT_MS = 45000;
exports.PROPERTIES_TIMEOUT_MS = PROPERTIES_TIMEOUT_MS;
// SMA devices commonly expose a single Modbus TCP slot and release it slowly.
// Throwaway pairing/repair sessions must be fully torn down before the owning
// device reconnects, otherwise the next connection may be accepted but ignored.
const MODBUS_SESSION_RELEASE_DELAY_MS = 3000;
exports.MODBUS_SESSION_RELEASE_DELAY_MS = MODBUS_SESSION_RELEASE_DELAY_MS;

// Waits for a throwaway device session (autoClose) to publish its properties.
// Every outcome explicitly disconnects and then allows the inverter time to
// release its Modbus slot before the caller starts or resumes another session.
exports.waitForProperties = function (smaSession, timeoutMs = PROPERTIES_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timer = null;

    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      smaSession.removeListener('properties', onProperties);
      smaSession.removeListener('error', onError);
    };

    const teardown = () => {
      if (typeof smaSession.disconnect === 'function') {
        try {
          smaSession.disconnect();
        } catch (_) {
          // Nothing useful to do; the session is being discarded anyway.
        }
      }
    };

    const finishAfterRelease = (callback) => {
      teardown();
      setTimeout(callback, MODBUS_SESSION_RELEASE_DELAY_MS);
    };

    const onProperties = (properties) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      finishAfterRelease(() => resolve(properties));
    };

    const onError = (err) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      const error = err instanceof Error ? err : new Error(exports.formatError(err));
      finishAfterRelease(() => reject(error));
    };

    smaSession.on('properties', onProperties);
    smaSession.on('error', onError);

    timer = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      const error = new Error(`Timed out after ${Math.round(timeoutMs / 100) / 10}s waiting for the device to report its properties`);
      finishAfterRelease(() => reject(error));
    }, timeoutMs);
  });
}

// jsmodbus tags every rejection with an `.err` code. These specific codes mean
// the request/response stream has gone out of sync on the shared socket: a
// request timed out (its late response is still coming), the transaction id of
// a response did not match the pending request, or the protocol/connection
// broke. Once this happens jsmodbus rejects the current request AND every other
// queued request with "rejecting because of earlier OutOfSync error", and the
// stale response left in the pipe keeps poisoning the next request every cycle.
// The only reliable recovery is to drop the socket so the OS buffer is flushed
// and a fresh client resets its transaction id counter.
const CONNECTION_POISONING_ERRORS = new Set(['Timeout', 'OutOfSync', 'Protocol', 'Offline']);

exports.isConnectionPoisoningError = function (err) {
  return !!(err && typeof err.err === 'string' && CONNECTION_POISONING_ERRORS.has(err.err));
}

// Extract the raw 16-bit word array from a jsmodbus readHoldingRegisters result.
exports.getRegisterWords = function (result) {
  return result && result.response && result.response._body
    ? result.response._body._valuesAsArray
    : undefined;
}

// Reads a single register (or multi-register value). Used both as a standalone
// read and as the per-register fallback when a coalesced range read is rejected.
// `registry` may be a plain numeric registryId (defaults to 2 registers) or a
// descriptor object { registryId, registerCount } for multi-register values (e.g. U64).
// Returns { values, poison, failed }:
//   - values: the register words, or a zero-filled array of the right length on
//     failure so the rest of the sweep still decodes (a marginal inverter that
//     times out on one register must not blank the whole sweep).
//   - poison: the failure desynchronized the request/response stream (see
//     isConnectionPoisoningError) so the caller can reset the socket once the
//     sweep finishes, avoiding an OutOfSync cascade on the next poll.
//   - failed: the read threw (as opposed to succeeding), used to track and log
//     per-register failure/recovery.
exports.modbusReading = async function (client, registry, deviceType, errorEmitter) {
  const isDescriptor = registry !== null && typeof registry === 'object';
  const registryId = isDescriptor ? registry.registryId : registry;
  const registerCount = (isDescriptor && registry.registerCount) ? registry.registerCount : 2;
  try {
    const result = await client.readHoldingRegisters(registryId, registerCount);
    return { values: exports.getRegisterWords(result), poison: false, failed: false };
  } catch (err) {
    if (errorEmitter && typeof errorEmitter._logMessage === 'function') {
      errorEmitter._logMessage('DEBUG', `Failed to read register '${registryId}': ${exports.formatError(err)}`);
    }
    // Prefer the session's guarded error funnel. A bare emit('error') throws when
    // no listener is attached, which is reachable here: a sweep can still be in
    // flight after the owning device called disconnect() and detached listeners.
    const readError = new Error(`Failed to read '${registryId}' for device type '${deviceType}'`);
    if (errorEmitter && typeof errorEmitter._emitError === 'function') {
      errorEmitter._emitError(readError);
    } else if (errorEmitter && typeof errorEmitter.emit === 'function' && errorEmitter.listenerCount('error') > 0) {
      errorEmitter.emit('error', readError);
    }

    return {
      values: new Array(registerCount).fill(0),
      poison: exports.isConnectionPoisoningError(err),
      failed: true
    };
  }
}

exports.isError = function (err) {
  return (err && err.stack && err.message);
}

// Robustly format any thrown / rejected value into a readable string.
// Avoids the "[object Object]" trap when:
//   - err is a plain object without .message
//   - err.message exists but is empty / non-string
//   - err is null / undefined / a primitive
//   - err contains circular references
exports.formatError = function (err) {
  if (err === null || err === undefined) {
    return 'Unknown error';
  }

  // Native Error (or anything Error-like with a usable message)
  if (err instanceof Error) {
    return err.message || err.toString() || 'Error';
  }

  // Strings / numbers / booleans
  if (typeof err !== 'object') {
    return String(err);
  }

  // jsmodbus UserRequestError shape: { err, message, request, response }
  if (typeof err.message === 'string' && err.message.length > 0) {
    if (typeof err.err === 'string' && err.err.length > 0) {
      return `${err.err}: ${err.message}`;
    }
    return err.message;
  }

  // Some libs use .code / .errno / .reason
  if (typeof err.code === 'string') {
    return err.code;
  }
  if (typeof err.reason === 'string') {
    return err.reason;
  }

  // Last resort: try JSON.stringify, guarding against circular refs
  try {
    const json = JSON.stringify(err);
    if (json && json !== '{}') {
      return json;
    }
  } catch (_) {
    // fall through
  }

  return 'Unknown error';
}