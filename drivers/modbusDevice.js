'use strict';

const BaseDevice = require('./baseDevice.js');
const utilFunctions = require('../lib/util.js');
const logger = require('../lib/logger.js');

const MINIMUM_AVAILABILITY_GRACE_PERIOD_MS = 5 * 60 * 1000;

class ModbusDevice extends BaseDevice {

    #lastDataReceived = null;
    #sessionStartedAt = null;
    #availabilityWatchdog = null;
    #retryCount = 0;
    #maxRetries = 30; // Maximum number of consecutive retry attempts
    #isReconnecting = false;
    #connectionGeneration = 0;
    #deleted = false;
    #repairSuspended = false;
    // Sliding window of recent read-error timestamps, used to detect a sustained
    // failure state and report it (once per interval) for blast-radius telemetry.
    #readErrorTimestamps = [];

    async onInit() {
        this.logMessage(`SMA device initiated`);
        this.api = null;

        await this.initializeSession(
            this.getSetting('address'),
            this.getSetting('port'),
            this.getSetting('polling'),
            this.getSetting('timeout')
        );
    }

    async initializeSession(address, port, polling, timeout) {
        // A lifecycle/settings initialization starts a new retry campaign. Any
        // pending callback from the previous settings is invalidated. A settings
        // update completed by repair also resumes the suspended live session.
        this.#repairSuspended = false;
        const generation = ++this.#connectionGeneration;
        this.#clearRetryTimer();
        this.#retryCount = 0;
        this.#isReconnecting = true;

        try {
            await this.#establishConnection(address, port, polling, timeout, generation);
        } catch (error) {
            if (!this.#isCurrentGeneration(generation)) {
                return;
            }

            this.error(`Failed to initialize device connection: ${utilFunctions.formatError(error)}`);
            await this.setUnavailable(utilFunctions.formatError(error) || 'Connection failed').catch(err => {
                this.error(`Failed to set device unavailable: ${utilFunctions.formatError(err)}`);
            });

            this.#isReconnecting = false;
            this.#scheduleReconnection(address, port, polling, timeout, generation);
        }
    }

    #isCurrentGeneration(generation) {
        return !this.#deleted && generation === this.#connectionGeneration;
    }

    // Shared connect sequence used by initial setup and retries. setupSession()
    // now waits for Modbus validation and the persistent socket, so reaching the
    // happy path means a real connection was established, not just an object built.
    async #establishConnection(address, port, polling, timeout, generation) {
        if (!this.#isCurrentGeneration(generation)) {
            return;
        }

        this.#stopAvailabilityWatchdog();
        this.#lastDataReceived = null;
        this.#sessionStartedAt = Date.now();

        await this.destroySession();
        if (!this.#isCurrentGeneration(generation)) {
            return;
        }

        await this.setupSession(address, port, polling, timeout);
        if (!this.#isCurrentGeneration(generation)) {
            return;
        }

        // Preserve retryCount until actual readings arrive. This prevents a
        // connect-without-communication cycle from restarting forever at 1/30.
        this.#isReconnecting = false;
        await this.setAvailable();
        this.#clearRetryTimer();
        this.#startAvailabilityWatchdog();
    }

    #clearRetryTimer() {
        if (this._retryTimeout) {
            this.homey.clearTimeout(this._retryTimeout);
            this._retryTimeout = null;
        }
    }

    async destroySession() {
        if (this.api) {
            const api = this.api;
            this.api = null;
            this.logMessage(`Disconnecting the device`);
            api.disconnect();
        }
    }

    async suspendConnectionForRepair() {
        if (this.#deleted || this.#repairSuspended) {
            return;
        }

        this.logMessage(`Suspending device connection for repair`);
        this.#repairSuspended = true;
        this.#connectionGeneration++;
        this.#clearRetryTimer();
        // Keep this true while suspended so watchdog/error paths cannot schedule
        // a replacement session during repair verification.
        this.#isReconnecting = true;
        this.#stopAvailabilityWatchdog();
        await this.destroySession();

        await new Promise(resolve => {
            this.homey.setTimeout(resolve, utilFunctions.MODBUS_SESSION_RELEASE_DELAY_MS);
        });
    }

    async resumeConnectionAfterRepair() {
        if (this.#deleted || !this.#repairSuspended) {
            return;
        }

        this.logMessage(`Resuming device connection after repair`);
        await this.initializeSession(
            this.getSetting('address'),
            this.getSetting('port'),
            this.getSetting('polling'),
            this.getSetting('timeout')
        );
    }

    onDeleted() {
        this.logMessage(`Deleting this SMA device from Homey.`);
        this.#deleted = true;
        this.#repairSuspended = false;
        this.#connectionGeneration++;
        this.#clearRetryTimer();
        this.#isReconnecting = false;
        this.#retryCount = 0;
        this.#sessionStartedAt = null;
        this.#stopAvailabilityWatchdog();
        this.destroySession();
    }

    // Schedules one reconnection at a time with exponential backoff.
    #scheduleReconnection(address, port, polling, timeout, generation = this.#connectionGeneration) {
        if (!this.#isCurrentGeneration(generation) || this.#isReconnecting) {
            return;
        }

        if (this.#retryCount >= this.#maxRetries) {
            this.logMessage(`Maximum retry attempts (${this.#maxRetries}) reached. Stopping reconnection attempts.`);
            return;
        }

        this.#retryCount++;
        const attempt = this.#retryCount;
        this.#isReconnecting = true;
        this.#clearRetryTimer();

        // Calculate exponential backoff delay: 30s, 1m, 2m, 4m, 8m, max 10m.
        const baseDelay = 30 * 1000;
        const maxDelay = 10 * 60 * 1000;
        const delay = Math.min(baseDelay * Math.pow(2, attempt - 1), maxDelay);

        this.logMessage(`Scheduling reconnection attempt ${attempt}/${this.#maxRetries} in ${Math.round(delay / 1000)}s`);

        this._retryTimeout = this.homey.setTimeout(async () => {
            // The handle no longer represents a pending timer once it fires.
            this._retryTimeout = null;
            if (!this.#isCurrentGeneration(generation)) {
                return;
            }

            this.logMessage(`Retry attempt ${attempt}/${this.#maxRetries}: Attempting to reconnect...`);
            try {
                await this.#establishConnection(address, port, polling, timeout, generation);
            } catch (err) {
                if (!this.#isCurrentGeneration(generation)) {
                    return;
                }

                this.error(`Reconnection attempt ${attempt}/${this.#maxRetries} failed: ${utilFunctions.formatError(err)}`);
                await this.setUnavailable(utilFunctions.formatError(err) || 'Connection failed').catch(setUnavailableError => {
                    this.error(`Failed to set device unavailable: ${utilFunctions.formatError(setUnavailableError)}`);
                });

                this.#isReconnecting = false;
                this.#scheduleReconnection(address, port, polling, timeout, generation);
            }
        }, delay);
    }

    async onSettings({ oldSettings, newSettings, changedKeys }) {
        let changeConn = false;
        let address, port, polling, timeout;
        if (changedKeys.indexOf("address") > -1) {
            this.logMessage(`Address value was change to '${newSettings.address}'`);
            changeConn = true;
            address = newSettings.address;
        }
        if (changedKeys.indexOf("port") > -1) {
            this.logMessage(`Port value was change to '${newSettings.port}'`);
            changeConn = true;
            port = newSettings.port;
        }
        if (changedKeys.indexOf("polling") > -1) {
            this.logMessage(`Polling value was change to '${newSettings.polling}'`);
            changeConn = true;
            polling = newSettings.polling;
        }
        if (changedKeys.indexOf("timeout") > -1) {
            this.logMessage(`Timeout value was change to '${newSettings.timeout}'`);
            changeConn = true;
            timeout = newSettings.timeout;
        }

        if (changeConn) {
            // Reinitialize and invalidate pending retries that captured old settings.
            await this.initializeSession(
                address || this.getSettings().address,
                port || this.getSettings().port,
                polling || this.getSettings().polling,
                timeout || this.getSettings().timeout
            );
        }
    }

    // Availability tracking methods
    #startAvailabilityWatchdog() {
        this.#stopAvailabilityWatchdog();

        const polling = this.getSetting('polling') || 10;
        // Check availability every refresh interval + 50% buffer
        const watchdogInterval = (polling * 1.5) * 1000;

        this.logMessage(`Starting availability watchdog with ${watchdogInterval / 1000}s interval`);

        this.#availabilityWatchdog = this.homey.setInterval(() => {
            this.#checkDataTimeout();
        }, watchdogInterval);
    }

    #stopAvailabilityWatchdog() {
        if (this.#availabilityWatchdog) {
            this.homey.clearInterval(this.#availabilityWatchdog);
            this.#availabilityWatchdog = null;
        }
    }

    async #checkDataTimeout() {
        const dataReferenceTime = this.#lastDataReceived || this.#sessionStartedAt;
        if (!dataReferenceTime) {
            return;
        }

        const now = Date.now();
        const polling = this.getSetting('polling') || 10;
        // Brief Modbus dropouts commonly recover on their own. Keep the device
        // available for at least five minutes while retaining its last valid
        // values; unusually long polling intervals still get two full cycles.
        const timeoutThreshold = Math.max(MINIMUM_AVAILABILITY_GRACE_PERIOD_MS, polling * 2 * 1000);
        const timeSinceLastData = now - dataReferenceTime;

        if (timeSinceLastData > timeoutThreshold && this.getAvailable()) {
            this.logMessage(`No data received for ${Math.round(timeSinceLastData / 1000)}s, marking as unavailable and attempting reconnection`);
            await this.setUnavailable('No data received from device').catch(err => {
                this.error(`Failed to set device unavailable: ${utilFunctions.formatError(err)}`);
            });

            this.#scheduleReconnection(
                this.getSetting('address'),
                this.getSetting('port'),
                this.getSetting('polling'),
                this.getSetting('timeout')
            );
        }
    }

    async onDataReceived() {
        this.#lastDataReceived = Date.now();

        // Valid data, rather than object construction or a bare TCP connection,
        // is the success signal that ends the retry campaign.
        this.#retryCount = 0;
        this.#isReconnecting = false;
        this.#clearRetryTimer();

        if (!this.getAvailable()) {
            this.logMessage(`Data received, marking device as available again`);
            try {
                await this.setAvailable();
            } catch (err) {
                this.error(`Failed to set device available: ${utilFunctions.formatError(err)}`);
            }
        }
    }

    // Method for child classes to call when communication errors occur
    async onCommunicationError(error) {
        // Telemetry: track error rate to detect sustained Modbus failures.
        this.#recordReadErrorForTelemetry(error);

        // Only mark as unavailable for actual communication/connectivity errors
        const isCommunicationError = this.#isCommunicationError(error);

        if (isCommunicationError && this.getAvailable()) {
            const formatted = utilFunctions.formatError(error);
            this.logMessage(`Communication error occurred, marking device as unavailable and attempting reconnection: ${formatted}`);
            await this.setUnavailable(`Communication error: ${formatted || 'Unknown error'}`).catch(err => {
                this.error(`Failed to set device unavailable: ${utilFunctions.formatError(err)}`);
            });

            // Trigger reconnection due to communication error
            this.#scheduleReconnection(
                this.getSetting('address'),
                this.getSetting('port'),
                this.getSetting('polling'),
                this.getSetting('timeout')
            );
        }
    }

    // Telemetry: records a read error and, when errors arrive in bursts (a
    // sustained failure rather than an occasional glitch), reports one event per
    // device per interval so we can see across installs which inverter models and
    // Homey firmware versions are affected. The logger rate-limits and is a no-op
    // when Sentry is not configured; this must never throw.
    #recordReadErrorForTelemetry(error) {
        try {
            const now = Date.now();
            const windowMs = 10 * 60 * 1000; // 10 minutes
            const threshold = 15; // errors within the window that indicate a sustained problem

            this.#readErrorTimestamps.push(now);
            this.#readErrorTimestamps = this.#readErrorTimestamps.filter(t => now - t <= windowMs);

            if (this.#readErrorTimestamps.length >= threshold) {
                let driverId = 'unknown';
                try { driverId = this.driver.id; } catch (_) { /* ignore */ }

                logger.report(
                    `modbus-sustained-failure:${this.getData().id}`,
                    'Sustained Modbus read failures',
                    {
                        level: 'warning',
                        tags: {
                            deviceType: this.getSetting('deviceType') || 'unknown',
                            driver: driverId,
                            homeyVersion: (this.homey && this.homey.version) || 'unknown',
                            // As tags (not just extra) so you can group/filter
                            // affected devices by their timeout / polling values.
                            polling: String(this.getSetting('polling')),
                            timeout: String(this.getSetting('timeout'))
                        },
                        extra: {
                            errorsInWindow: this.#readErrorTimestamps.length,
                            windowMinutes: 10,
                            polling: this.getSetting('polling'),
                            timeoutSetting: this.getSetting('timeout'),
                            lastError: utilFunctions.formatError(error)
                        }
                    }
                );
            }
        } catch (_) {
            // Telemetry must never affect device operation.
        }
    }

    // Helper method to determine if an error is communication-related
    #isCommunicationError(error) {
        if (!error || !error.message) {
            return false;
        }

        const errorMessage = error.message.toLowerCase();
        const communicationErrorPatterns = [
            'is not reachable',
            'connection failed',
            'connection refused',
            'connection reset',
            'connection timeout',
            'network is unreachable',
            'host is unreachable',
            'no route to host',
            'econnrefused',
            'etimedout',
            'econnreset',
            'ehostunreach',
            'enetunreach',
            'enotfound'
        ];

        return communicationErrorPatterns.some(pattern => errorMessage.includes(pattern));
    }
}
module.exports = ModbusDevice;
