/**
 * Decaying counter storage for kad-ratelimiter.
 */

'use strict';
var P = require('bluebird');
var assert = require('assert');

/**
 * Creates an in-memory kad storage adapter
 * @constructor
 */
function MemoryBackend(options) {
    this._options = options || {};
    assert(this._options.interval, 'options.interval is required');
    if (!this._options.minValue) {
        this._options.minValue = 0.1;
    }

    this._counters = {};
    // Exponential decay with factor 2, thrice per interval for better
    // smoothness, using the cubic root of 2).
    this._decayFactor = Math.pow(2, 1/3);

    // Start the periodic decay process.
    var self = this;
    this._decayCountersInterval = setInterval(function() {
        self._decayCounters();
    }, self._options.interval / 3);
}

/**
 * Decay all counters, and remove those that dropped below minValue.
 */
MemoryBackend.prototype._decayCounters = function() {
    var minValue = this._options.minValue;
    var counters = this._counters;
    var keys = Object.keys(counters);
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        counters[key] /= this._decayFactor;
        if (counters[key] < minValue) {
            delete counters[key];
        }
    }
};

MemoryBackend.prototype.setup = function() {
    return P.resolve(this);
};

MemoryBackend.prototype.stop = function() {
    clearInterval(this._decayCountersInterval);
};

MemoryBackend.prototype.get = function(key) {
    return P.resolve(this._counters[key] || 0);
};

MemoryBackend.prototype.put = function(key, value) {
    if (this._counters[key]) {
        this._counters[key] += value;
    } else {
        this._counters[key] = value;
    }
    return P.resolve(this._counters[key]);
};


module.exports = MemoryBackend;
