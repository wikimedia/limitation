'use strict';
var P = require('bluebird');

var kad = P.promisifyAll(require('kad'));
var DecayingCounterStore = require('./decaying_counter_store');

function KadBackend(options) {
    if (!options.listen) {
        options.listen = { address: 'localhost', port: 3050 };
    }
    if (!options.listen.address) { options.listen.address = 'localhost'; }
    if (!options.listen.port) { options.listen.port = 3050; }
    if (options.minValue === undefined) {
        options.minValue = 0.1;
    }

    this._options = options;
    this._onMasterPort = false;
    this._running = false;
    this._dht = null;
    this._storage = null;
}

KadBackend.prototype.setup = function() {
    var self = this;
    if (!self._onMasterPort) {
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

            // TODO: Support forwarding logs via options.log
            var logger = new kad.Logger(0, 'kad-example' + Math.random());
            self._storage = new DecayingCounterStore(self._options);
            self._dht = kad.Node({
                transport: transport,
                logger: logger,
                storage: self._storage,
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
            return self;
        })
        .catch(function(err) {
            console.log('Error during DHT setup', err);
        });
    }
};

KadBackend.prototype.stop = function() {
    if (this._storage) { this._storage.stop(); }
};

KadBackend.prototype._setupTransport = function(listen, retries) {
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
    })
    .then(function(transport) {
        // Set up a permanent error handler, to avoid low-level errors being
        // converted into (uncaught) async exceptions.
        transport.on('error', function(e) {
            // console.log(e);
        });
        return transport;
    });
};

/**
 * Randomize the configured interval slightly
 */
KadBackend.prototype._getRandomizedInterval = function(multiplier) {
    var interval = this._options.interval * (multiplier || 1);
    return interval + (Math.random() - 0.5) * interval * 0.1;
};

/**
 * Update counters
 * @param {string} key
 * @param {number} value
 * @return {Promise<value>} Current counter value
 */
KadBackend.prototype.put = function(key, value) {
    return this._dht.putAsync(key, value);
};

KadBackend.prototype.get = function(key, value) {
    return this._dht.getAsync(key);
};

module.exports = KadBackend;
