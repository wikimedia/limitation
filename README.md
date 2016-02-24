# Limitation

An efficient rate limiter with several backends, including a Kademlia DHT.

## Features

- Fully synchronous local limit checking for lowest latency and high
    throughput.
- Scales constant in number of requests, and linear in number of keys.
- Resilient Kademlia DHT storage backend using an exponentially decaying
  counter.
