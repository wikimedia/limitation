/**
 * Decaying counter storage for kad-ratelimiter.
 */

'use strict';

var ReadableStream = require('readable-stream');

/**
 * Creates an in-memory kad storage adapter
 * @constructor
 */
function DecayingCounterStore(options) {
    if (!(this instanceof DecayingCounterStore)) {
        return new DecayingCounterStore(options);
    }

    this._options = options || {};
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

DecayingCounterStore.prototype.stop = function() {
    clearInterval(this._decayCountersInterval);
};

/**
 * Decay all counters, and remove those that dropped below minValue.
 */
DecayingCounterStore.prototype._decayCounters = function() {
    var self = this;
    var minValue = self._options.minValue;
    var keys = Object.keys(this._counters);
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var counter = self._counters[key];
        // console.log('decay', key, counter.value);
        counter.value /= self._decayFactor;
        if (counter.value < minValue) {
            delete self._counters[key];
        }
    }
};

/**
 * Gets an item from the store
 * #get
 * @param {string} key
 * @param {function} callback
 */
DecayingCounterStore.prototype.get = function(key, callback) {
    var self = this;
    callback(null, self._get(key) || null);
};

DecayingCounterStore.prototype._get = function(key) {
    var counter = this._counters[key];
    if (counter) {
        return JSON.stringify(counter);
    } else {
        return null;
    }
};

/**
 * Puts an item into the store
 * #put
 * @param {string} key
 * @param {string} value
 * @param {function} callback
 */
DecayingCounterStore.prototype.put = function(key, value, callback) {
    var self = this;
    var parsedValue = JSON.parse(value);
    // console.log(parsedValue);
    var storedValue = this._counters[key];
    if (storedValue) {
        // Increment the value
        // console.log('increment', storedValue.value, parsedValue.value);
        storedValue.value += parsedValue.value;
        // Update the timestamp
        storedValue.timestamp = parsedValue.timestamp;
    } else {
        this._counters[key] = parsedValue;
    }

    callback(null, self._get(key));
};

/**
 * Deletes an item from the store
 * #del
 * @param {string} key
 * @param {function} callback
 */
DecayingCounterStore.prototype.del = function(key, callback) {
    delete this._counters[key];
    callback(null);
};

/**
 * Returns a readable stream of items
 * #createReadStream
 */
DecayingCounterStore.prototype.createReadStream = function() {
    var adapter = this;
    var items = Object.keys(this._counters);
    var current = 0;

    return new ReadableStream({
        objectMode: true,
        read: function() {
            var stream = this;
            var key = items[current];

            if (!key) {
                return stream.push(null);
            }

            setImmediate(function pushItem() {
                current++;
                stream.push({ key: key, value: adapter._get(key) });
            });
        }
    });
};

module.exports = DecayingCounterStore;
