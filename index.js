/**
 * Kademlia DHT based rate limiter.
 *
 * Features:
 * - Checks are cheap in-memory operations.
 * - Distributes counters across many nodes, using the Kademlia DHT.
 * - Supports multiple limits per key.
 * - Configurable bursting and update scaling via `interval` option.
 */

'use strict';

var events = require('events');
var util = require('util');
var P = require('bluebird');

var MemoryBackend = require('./lib/memory_backend');
var KadBackend = require('./lib/kad_backend');

/**
 * Limitation constructor
 *
 * @param {object} options:
 * - `listen`: {object} describing the local interface to listen on. Default:
 *   `{ address: 'localhost', port: 3050 }`. If this port is used, a random
 *   port is used instead.
 * - `seeds`: {[object]} describing seeds nodes, containing `port` and
 *   `address` string properties
 * - `interval`: Update interval in ms. Default: 10000ms. Longer intervals
 *   reduce load, but also increase detection latency.
 * - `minValue`: Drop global counters below this value. Default: 0.1.
 */
function Limitation(options) {
    events.EventEmitter(this);
    this._options = options = options || {};
    if (options.interval === undefined) {
        options.interval = 10000;
    }

    // Local counters. Contain objects with `value` and `limits` properties.
    this._counters = {};
    this._blocks = {};

    this._end = false;
    this._globalUpdatesTimeout = null;

    if (!options.seeds || !options.seeds.length) {
        // Single-node operation
        this._store = new MemoryBackend(options);
    } else if (options.seeds && options.seeds.length) {
        this._store = new KadBackend(options);
    }
}

util.inherits(Limitation, events.EventEmitter);


/**
 * Synchronous limit check
 *
 * @param {string} key
 * @param {number} limit
 * @param {number} increment, default 1
 * @return {boolean}: `true` if the request rate is below the limit, `false`
 * if the limit is exceeded.
 */
Limitation.prototype.isAboveLimit = function(key, limit, increment) {
    var counter = this._counters[key];
    if (!counter) {
        counter = this._counters[key] = {
            value: 0,
            limits: {},
        };
    }
    counter.value += increment || 1;
    counter.limits[limit] = counter.limits[limit] || Date.now();

    if (this._blocks[key]) {
        return this._blocks[key].value > limit;
    } else {
        return false;
    }
};

/**
 * Set up / connect the limiter.
 * @returns {P<Limitation>
 */
Limitation.prototype.setup = function() {
    var self = this;

    return self._store.setup()
    .then(function(store) {
        self._store = store;
        // Start periodic global updates
        self._globalUpdatesTimeout = setTimeout(function() {
            return self._globalUpdates();
        }, self._getRandomizedInterval(0.5));

        return self;
    });
};

Limitation.prototype.stop = function() {
    this._end = true;
    this._store.stop();
    clearTimeout(this._globalUpdatesTimeout);
};

/**
 * Randomize the configured interval slightly
 */
Limitation.prototype._getRandomizedInterval = function(multiplier) {
    var interval = this._options.interval * (multiplier || 1);
    return interval + (Math.random() - 0.5) * interval * 0.1;
};


/**
 * Report local counts to the global DHT, and update local blocks.
 */
Limitation.prototype._globalUpdates = function() {
    var self = this;
    // Set up an empty local counter object for the next interval
    var lastCounters = self._counters;
    self._counters = {};

    // New blocks. Only update these after the full iteration.
    var newBlocks = {};
    // For each local counter, update the DHT & check for limits
    var errCount = 0;
    return P.map(Object.keys(lastCounters), function(key) {
        var counter = lastCounters[key];
        return self._store.put(key, counter.value)
        .then(function(counterVal) {
            counterVal = self._normalizeCounter(counterVal);
            var minLimit = Math.min.apply(null, Object.keys(counter.limits));
            // console.log('put val', counterVal, minLimit, counter.value);
            if (counterVal > minLimit) {
                newBlocks[key] = {
                    value: counterVal,
                    limits: counter.limits
                };
            }
        })
        // Ignore update errors.
        .catch(function() {
            errCount++;
        });
    }, { concurrency: 50 })
    .then(function() {
        return self._updateBlocks(newBlocks);
    })
    .catch(function(err) {
        console.log(err.stack);
    })
    .finally(function(err) {
        self.emit('blocks', self._blocks);
        // Schedule the next iteration
        if (self._end) { return; }
        self._globalUpdatesTimeout = setTimeout(function() {
            self._globalUpdates();
        }, self._getRandomizedInterval());
    });
};

Limitation.prototype._normalizeCounter = function(val) {
    val = val || 0;
    // Compensate for exponential decay with factor 2, and scale to 1/s rates.
    // Bias against false negatives by diving by 2.2 instead of 2.0.
    return val / 2.2 / this._options.interval * 1000;
};

/**
 * Re-check old blocks against newBlocks and the DHT state.
 *
 * This method ensures that we keep blocking requests when the global request
 * rate is a bit above the limit, even if the local request rates occasionally
 * drops below the limit.
 *
 * @param {object} newBlocks, new blocks based on the put response from local counters.
 */
Limitation.prototype._updateBlocks = function(newBlocks) {
    var self = this;
    // Stop checking for old limits if they haven't been reached in the last
    // 600 seconds.
    var maxAge = Date.now() - (600 * 1000);

    var asyncChecks = [];
    var oldBlocks = this._blocks;
    var oldBlockKeys = Object.keys(self._blocks);

    // Fast handling for blocks that remain
    for (var i = 0; i < oldBlockKeys.length; i++) {
        var key = oldBlockKeys[i];
        if (newBlocks[key]) {
            var newBlockLimits = newBlocks[key].limits;
            // Still blocked. See if other limits need to be added.
            var oldLimits = self._blocks[key].limits;
            var oldLimitKeys = Object.keys(oldLimits);
            for (var j = 0; j < oldLimitKeys.length; j++) {
                var limit = oldLimitKeys[j];
                if (oldLimits[limit] > maxAge && !newBlockLimits[limit]) {
                    newBlockLimits[limit] = oldLimits[limit];
                }
            }
        } else {
            asyncChecks.push(key);
        }
    }

    self._blocks = newBlocks;

    // Async re-checks for previous blocks that didn't see any requests in the
    // last interval.
    return P.map(asyncChecks, function(key) {
        var block = oldBlocks[key];
        // Only consider
        var currentLimits = Object.keys(block.limits)
            .filter(function(limit) {
                return block.limits[limit] > maxAge;
            });
        if (!currentLimits.length) {
            // Nothing to do.
            return;
        }

        // Need to get the current value
        return self._store.get(key)
        .then(function(counterVal) {
            counterVal = self._normalizeCounter(counterVal);
            var limitObj = {};
            var curTime = Date.now();
            currentLimits.forEach(function(limit) {
                if (Number(limit) > counterVal) {
                    limitObj[limit] = curTime;
                } else {
                    limitObj[limit] = block.limits[limit];
                }
            });

            newBlocks[key] = {
                value: counterVal,
                limits: limitObj,
            };
        })
        // Ignore individual update errors.
        .catch(function() {});
    }, { concurrency: 50 });
};




module.exports = Limitation;
