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

var kad = P.promisifyAll(require('kad'));
var DecayingCounterStore = require('./lib/decaying_counter_store');

/**
 * RateLimiter constructor
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
function RateLimiter(options) {
    events.EventEmitter(this);
    this._options = options || {};
    if (!options.listen) {
        options.listen = { address: 'localhost', port: 3050 };
    }
    if (!options.listen.address) { options.listen.address = 'localhost'; }
    if (!options.listen.port) { options.listen.port = 3050; }
    if (options.minValue === undefined) {
        options.minValue = 0.1;
    }
    if (options.interval === undefined) {
        options.interval = 10000;
    }

    // seeds: [{ address, port }]

    // Local counters. Contain objects with `value` and `limits` properties.
    this._counters = {};
    this._blocks = {};

    this._onMasterPort = false;
}

util.inherits(RateLimiter, events.EventEmitter);


/**
 * Synchronous limit check
 *
 * @param {string} key
 * @param {number} limit
 * @param {number} increment, default 1
 * @return {boolean}: `true` if the request rate is below the limit, `false`
 * if the limit is exceeded.
 */
RateLimiter.prototype.check = function(key, limit, increment) {
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
        return this._blocks[key].value < limit;
    } else {
        return true;
    }
};

/**
 * Set up / connect the RateLimiter.
 * @returns {P<RateLimiter>
 */
RateLimiter.prototype.setup = function() {
    var self = this;
    if (!self._dht) {
        // Start periodic global updates
        setTimeout(function() {
            return self._globalUpdates();
        }, self._getRandomizedInterval(0.5));

        // Periodically update the number of contacts
        // setInterval(function() {
        //     self._num_contacts = self._dht._router
        //         .getNearestContacts('', 1000, self._dht._self).length;
        //     console.log('contacts', self._num_contacts);
        // }, self._getRandomizedInterval(2));
    }

    if (!self._dht || !self._onMasterPort) {
        var masterPort = self._options.listen.port;
        return self._setupTransport(self._options.listen)
        .then(function(transport) {
            self._onMasterPort = transport._contact.port === masterPort;
            // Schedule a re-connect
            if (!self._onMasterPort) {
                setTimeout(self.setup.bind(self), self._getRandomizedInterval(60));
                if (self._dht) {
                    // Already connected, but can't switch to master port. Do
                    // not replace the current transport instance.
                    transport.close();
                    return;
                }
            }

            var logger = new kad.Logger(2, 'kad-example' + Math.random());
            self._dht = kad.Node({
                transport: transport,
                logger: logger,
                storage: new DecayingCounterStore(self._options)
            });
            self._options.seeds.forEach(function(seed) {
                if (typeof seed === 'string') {
                    seed = {
                        address: seed,
                        port: 3050,
                    };
                }
                if (transport._contact.port !== seed.port
                        || transport._contact.address !== seed.address) {
                    self._dht.connect(seed);
                }
            });
            self._num_contacts = self._options.seeds.length;
            return self;
        })
        .catch(function(err) {
            console.log('Error during DHT setup', err);
        });
    }
};

/**
 * Randomize the configured interval slightly
 */
RateLimiter.prototype._getRandomizedInterval = function(multiplier) {
    var interval = this._options.interval * (multiplier || 1);
    return interval + (Math.random() - 0.5) * interval * 0.1;
};

RateLimiter.prototype._setupTransport = function(listen, retries) {
    var self = this;
    if (retries === 0) {
        return P.reject();
    }

    return new P(function(resolve) {
        if (retries) {
            // Retry on a random port
            listen = {
                address: listen.address,
                port: 1024 + Math.floor(Math.random() * 63000),
            };
        }

        var transport = new kad.transports.UDP(
                kad.contacts.AddressPortContact(listen));
        transport.once('error', function() {
            if (retries === undefined) {
                retries = 5;
            } else {
                retries--;
            }
            resolve(self._setupTransport(listen, retries));
        });
        transport.once('ready', function() {
            resolve(transport);
        });
    });
};


/**
 * Report local counts to the global DHT, and update local blocks.
 */
RateLimiter.prototype._globalUpdates = function() {
    var self = this;
    // Set up an empty local counter object for the next interval
    var lastCounters = self._counters;
    self._counters = {};

    if (!self._dht) {
        return P.resolve();
    }

    // New blocks. Only update these after the full iteration.
    var newBlocks = {};
    // For each local counter, update the DHT & check for limits
    var errCount = 0;
    return P.map(Object.keys(lastCounters), function(key) {
        var counter = lastCounters[key];
        return self._dht.putAsync(key, counter.value)
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
        setTimeout(function() {
            self._globalUpdates();
        }, self._getRandomizedInterval());
    });
};

RateLimiter.prototype._normalizeCounter = function(val) {
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
RateLimiter.prototype._updateBlocks = function(newBlocks) {
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
        return self._dht.getAsync(key)
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




module.exports = RateLimiter;
