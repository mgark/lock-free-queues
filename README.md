
# Introduction

Fairly generic lock-free templated lock-free implemenation for SPSC, MPMC, MPSC, SCMP. The idea is to provide the same interface for all of those queue and allow multiple complile-time options to tune them for real scenarios.
Also sequential publishing is supported for multi-producer scenario with a compile time flag.

# Features

- single producer / consumer interface for all kinds of queues
- sequential mutti-publishers supported
- natural conflation support - publishers could override data
- it is pretty pretty fast.
- multiple compile-time tweaks
- supports reading data through input iterators
- dynamic consumers join / detach
- many ways to consume data - return by value, lamba processing, return by pointer etc
- supports peeking elements by consumers
- allocator friendly

# TODO

- implement slow consumer handling by allowing ring queue to grow dynamically - up to a limit. Also with this feature you'd get pretty much unbounded queue as well and it would be very fast.
- benchmark against other implementations, including Java's Distruptor
- micro optimizations for optimal assembly output including relaxing some of the atomics
- test the queues on the devices with weak memory ordering to catch any possible race condition
- use Race Relay detector to spot any tricky race condition
- make publishers work with output iterators
