'use strict';

const HomeyEventEmitter = require('./homeyEventEmitter.js');
const util = require('../util.js');
const dgram = require("dgram");

//UDP datagram must be sent to the multicast address 239.12.255.254 via port 9522
const PORT = 9522;
const MULTICAST_ADDR = "239.12.255.254";
const datagram = Buffer.from('534d4100000402a0ffffffff0000002000000000', 'hex');

class BaseDiscovery extends HomeyEventEmitter {
    constructor(options = {}) {
        super();
        this.options = options;
        this.deviceList = [];
        this.devicePort = options.port;
    }

    // Abstract method - must be implemented by subclasses
    createDevice(options) {
        throw new Error('createDevice method must be implemented by subclass');
    }

    // Abstract method - must be implemented by subclasses  
    getDeviceTypeName() {
        throw new Error('getDeviceTypeName method must be implemented by subclass');
    }

    discover() {
        return new Promise((resolve) => {
            const socket = dgram.createSocket({ type: "udp4", reuseAddr: true });
            let socketClosed = false;
            let settled = false;

            // The socket is bound to a fixed port, so it has to be released when
            // discovery is done. Leaking it left an extra listener on 9522 for the
            // rest of the app's lifetime and, because the SMA reply is unicast back
            // to that port, a later discovery's replies could be delivered to the
            // orphaned socket instead - indistinguishable from "found 0 devices".
            const closeSocket = () => {
                if (socketClosed) {
                    return;
                }
                socketClosed = true;
                try {
                    socket.close();
                } catch (error) {
                    this._logMessage('DEBUG', `Discovery socket already closed: ${util.formatError(error)}`);
                }
            };

            const settle = (devices) => {
                if (settled) {
                    return;
                }
                settled = true;
                closeSocket();
                resolve(devices);
            };

            // Without this handler a failure on the datagram socket (port 9522
            // already in use, no interface for the multicast group) is emitted as
            // an unhandled 'error' event, which takes the whole app down instead of
            // falling back to manual entry.
            socket.on("error", (error) => {
                this._logMessage('WARN', `Discovery socket error, auto-discovery unavailable: ${util.formatError(error)}`);
                settle([]);
            });

            socket.on("listening", () => {
                try {
                    socket.addMembership(MULTICAST_ADDR);
                } catch (error) {
                    this._logMessage('WARN', `Unable to join multicast group ${MULTICAST_ADDR}: ${util.formatError(error)}`);
                    settle([]);
                    return;
                }
                socket.send(datagram, 0, datagram.length, PORT, MULTICAST_ADDR, (error) => {
                    if (error) {
                        this._logMessage('WARN', `Failed to send discovery datagram: ${util.formatError(error)}`);
                        settle([]);
                        return;
                    }
                    this._logMessage('INFO', `Sending discovery datagram`);
                });
            });

            socket.on("message", (message, rinfo) => {
                this._logMessage('DEBUG', `Message from: ${rinfo.address}:${rinfo.port} - ${message}`);
                const response = Buffer.from(message).toString('hex');
                if (response.startsWith('534d4100000402a000000001000200000001')) {
                    this.deviceList.push(rinfo.address);
                    this._logMessage('INFO', `Found SMA device at: ${rinfo.address}`);
                }
            });

            socket.bind(PORT);
            //Wait 2 seconds before we collect the devices found
            this._sleep(2000).then(() => {
                if (settled) {
                    return;
                }
                this._logMessage('INFO', `Collecting device info, found ${this.deviceList.length} devices.`);
                // No further datagrams are needed; release the port before the
                // Modbus lookups, which can take several seconds per device.
                closeSocket();
                if (this.deviceList.length > 0) {
                    this.#lookupDevices(this.deviceList)
                        .then(validDevices => {
                            settle(validDevices);
                        })
                        .catch(error => {
                            // Even if some lookups fail, we still want to return the successful ones
                            this._logMessage('DEBUG', `Device lookup failed: ${util.formatError(error)}`);
                            settle([]);
                        });
                } else {
                    settle([]);
                }
            });
        });
    }

    async #lookupDevices(ipAddresses) {
        const validDevices = [];
        const lookupPromises = ipAddresses.map(async ipAddress => {
            try {
                const deviceInfo = await this.#validateAndGetProperties(ipAddress);
                validDevices.push(deviceInfo);
            } catch (error) {
                // Log the error but continue with other lookups
                this._logMessage('WARN', error.message);
            }
        });

        await Promise.all(lookupPromises);
        return validDevices;
    }

    async #validateAndGetProperties(ipAddress) {
        // First, quickly validate connectivity using util function
        this._logMessage('DEBUG', `Validating connectivity to ${ipAddress}:${this.devicePort}`);
        const isAvailable = await util.isModbusAvailable(ipAddress, this.devicePort, this._logMessage.bind(this));

        if (!isAvailable) {
            throw new Error(`${this.getDeviceTypeName()} found on IP '${ipAddress}', but port '${this.devicePort}' is not reachable!`);
        }

        // If connectivity is good, get device properties
        this._logMessage('DEBUG', `Connectivity validated, getting device properties from ${ipAddress}`);

        try {
            const smaSession = this.createDevice({
                host: ipAddress,
                port: this.devicePort,
                autoClose: true,
                device: this.options.device
            });

            // Wait for the properties event, bounded. Without a deadline a single
            // unresponsive device would leave Promise.all in #lookupDevices - and
            // therefore discover() and the pairing view - hanging indefinitely.
            const properties = await util.waitForProperties(smaSession);

            // Add connection details to the properties
            properties.port = this.devicePort;
            properties.address = ipAddress;
            this._logMessage('DEBUG', `Retrieved properties for ${properties.deviceType} at ${ipAddress}`);

            return properties;

        } catch (error) {
            if (this.options.device && typeof this.options.device.error === 'function') {
                this.options.device.error(util.formatError(error));
            }
            this._logMessage('WARN', `Failed to get properties from ${ipAddress}: ${util.formatError(error)}`);
            throw new Error(`${this.getDeviceTypeName()} at IP '${ipAddress}' failed to provide properties: ${util.formatError(error)}`, { cause: error });
        }
    }
}

module.exports = BaseDiscovery;
