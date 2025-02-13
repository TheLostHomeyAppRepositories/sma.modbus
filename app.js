"use strict";

const { App } = require('homey');
const { Log } = require('homey-log');

class SmaModbusApp extends App {

  async onInit() {
    this.homeyLog = new Log({ homey: this.homey });
    this.setupGlobalFetch();
    this.log('Initializing SMA Modbus app ...');
  }

  setupGlobalFetch() {
    if (!global.fetch) {
      global.fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
    }
    if (!global.AbortSignal.timeout) {
      global.AbortSignal.timeout = timeout => {
        const controller = new AbortController();
        const abort = setTimeout(() => {
          controller.abort();
        }, timeout);
        return controller.signal;
      }
    }
  }
}

module.exports = SmaModbusApp;
