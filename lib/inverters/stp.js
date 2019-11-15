'use strict';

const {ModbusRegistry, type, setting} = require('../inverterConst.js');
const {BASE} = require('./base.js');

//Used by
//STP 5000TL-20, STP 6000TL-20, STP 7000TL-20, STP 8000TL-20, STP 9000TL-20
//STP 10000TL-20, STP 12000TL-20
//STP 15000TL-30, STP 20000TL-30, STP 25000TL-30
//STP 12000TL-US-10, STP 15000TL-US-10, STP 20000TL-US-10, STP 24000TL-US-10, 
//STP 30000TL-US-10
const STP_int = {
};
const STP = Object.freeze(Object.assign(Object.assign({}, BASE), STP_int));

//Used by
//STP 8000TL-10, STP 10000TL-10, STP 12000TL-10, STP 15000TL-10, STP 17000TL-10
//Missing Operating status
//Missing A voltage
//Missing B voltage
const STP_TL10_int = {
    status: null,
    dcVoltageA: null,
    dcVoltageB: null,
};
const STP_TL10 = Object.freeze(Object.assign(Object.assign({}, STP), STP_TL10_int));

//Used by
//STP 24500TL-JP-30, STP 25000TL-JP-30
//Different value for AC power L1 (30789)
const STP_JP_int = {
    acVoltageL1: new ModbusRegistry(setting.READING, 30789, type.U32_FIX2, 'AC Voltage L1', 'measure_voltage')
};
const STP_JP = Object.freeze(Object.assign(Object.assign({}, STP), STP_JP_int));

//Used by
//STP 15000TLEE-10, STP 20000TLEE-10
//Missing B voltage
//Missing Operating status
const STP_TLEE_int = {
    status: null,
    dcVoltageB: null,
};
const STP_TLEE = Object.freeze(Object.assign(Object.assign({}, STP), STP_TLEE_int));

//Used by
//STP 10000TLEE-JP-10, STP 10000TLEE-JP-11, STP 20000TLEE-JP-11
//Missing B voltage
//Different value for AC power L1 (30789)
const STP_TLEE_JP_int = {
    dcVoltageB: null,
    acVoltageL1: new ModbusRegistry(setting.READING, 30789, type.U32_FIX2, 'AC Voltage L1', 'measure_voltage')
};
const STP_TLEE_JP = Object.freeze(Object.assign(Object.assign({}, STP), STP_TLEE_JP_int));

//Used by
//STP 50-US-40, STP 50-JP-40
//STP 3.0-3AV-40, STP 4.0-3AV-40, STP 5.0-3AV-40, STP 6.0-3AV-40, STP 8.0-3AV-40, STP 10.0-3AV-40
//Missing daily yield
const STP_40_int = {
    dailyYield: null,
};
const STP_40 = Object.freeze(Object.assign(Object.assign({}, STP), STP_40_int));

module.exports = {
    STP,
    STP_TL10,
    STP_JP,
    STP_TLEE,
    STP_TLEE_JP,
    STP_40
}