# Limitation

An efficient rate limiter with several backends, including a Kademlia DHT.

## Features

- Fully synchronous local limit checking for lowest latency and high
    throughput. Using node 4.3 on a single core, a throughput of about 8
    million limit checks per second is typical.
- Backends scale constant in number of requests, and linear in number
    of keys. Tested with thousands of distinct limits.
- Backends:
    - Resilient Kademlia DHT storage backend using an exponentially decaying
      counter.
    - Simple and very fast local in-memory storage backend.

## Installation
```bash
npm install limitation
```

## Usage

Simple in-memory backend:

```javascript
var Limitation = require('limitation');

var ratelimiter = new Limitation();

if (ratelimiter.isAboveLimit('some_key', 10)) {
  console.log('Limit of 10 req/s exceeded!');
}
```

Kademlia backend:
```javascript
var Limitation = require('limitation');

var ratelimiter = new Limitation({
    seeds: [{
        address: 'localhost',
        port: 3050
    },
    // Host names are also supported, expands to some.host:3050
    'some.host'],

    // Optional parameters
    // Address / port to listen on
    // Default: localhost:3050, random port fall-back if port used
    listen: {
        address: '127.0.0.1',
        port: 3050
    },
    // Counter update / block interval; Default: 10000ms
    // Shorter interval means quicker limiting, but also higher load.
    interval: 10000
});

// Returns Promise<Limitation>
ratelimiter.setup();

if (ratelimiter.isAboveLimit('some_key', 10)) {
  console.log('Limit of 10 req/s exceeded!');
}

// Clears any set timers so the process can exit cleanly.
ratelimiter.stop()
```

See also [test/index.js](test/index.js) for a runnable Kademlia example. It
starts multiple DHT nodes, and you can start multiple copies of the script to
play with nodes going down etc.
