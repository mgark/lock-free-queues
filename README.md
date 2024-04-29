
# Introduction

Fairly generic lock-free templated lock-free implemenation for SPSC, MPMC, MPSC, SCMP. The idea is to provide the same interface for all of those queue and allow multiple complile-time options to tune them for real scenarios.
Also sequential publishing is supported for multi-producer scenario with a compile time flag.


# Requirements 

- at least C++20 and gcc - got issues with aggregate initializaiton on clang (would address later)
- Catch2 for tests
- CMake for building

# Features

- lock-free / wait-free. There is only one tiny usage of spin-locks on the slow path
- dynamic consumers join / detach
- single producer / consumer interface for all kinds of queues
- sequential mutti-publishers supported
- [in reliable delivery mode] if there is no consumer attached to the queue, producers would just keep pushing...only when there is at least one consumer, producer would be blocked
- natural conflation support - publishers could override data
- it is pretty pretty fast.
- multiple compile-time tweaks
- supports reading data through input iterators
- many ways to consume data - return by value, lamba processing, return by pointer etc
- supports peeking elements by consumers
- allocator friendly

# TODO

- full clang support
- implement slow consumer handling by allowing ring queue to grow dynamically - up to a limit. Also with this feature you'd get pretty much unbounded queue as well and it would be very fast.
- benchmark against other implementations, including Java's Distruptor
- micro optimizations for optimal assembly output including relaxing some of the atomics
- test the queues on the devices with weak memory ordering to catch any possible race condition
- use Relacy Race Detector to spot any potential race condition
- make publishers work with output iterators
