# kad-ratelimiter

A distributed rate limiter implemented on top of the kadtools kademlia DHT.

## Features

- Fully synchronous local limit checking for lowest latency and high
    throughput.
- Scales constant in number of requests, and linear in number of keys.
- Resilient Kademlia DHT storage using an exponentially decaying counter
    backend.
