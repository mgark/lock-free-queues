
# Introduction

Fairly generic templated lock-free implemenation for SPSC, MPMC, MPSC, SCMP. The idea is to provide the same interface for all of those queues and allow multiple compile-time options to tune them for real scenarios.
Also sequential publishing is supported for multi-producer scenarios with a compile time setting.

# Design

## Reliable queues

The idea behind the implementation is pretty simple - just have a fixed number of slots in a ring buffer with fixed size, and don't let consumers touch the memory from the ring buffer. Instead let it update its read index at intervals (default is each 25% of the ring buffer size)!

## Conflated queues

Consumers don't need to track their index even, just check if a given slot got a newer version, copy it and after the copy has been finished make sure the version has not changed as the publisher might have warped around by the time the copy finished.

# Requirements

- at least C++20
- Catch2 for tests
- CMake for building

# Features

- reliable (if there is at least one consumer live, publishers won't be able to override the data and would basically stop progress until there is space on the ring) and conflated queue types (publishers can override data in case consumers are slow)
- lock-free / wait-free. There is only one tiny usage of spin-locks on the slow path
- dynamic consumers join / detach
- single producer / consumer interface for all kinds of queues
- sequential mutti-publishers supported
- it is pretty pretty fast.
- multiple compile-time tweaks
- supports reading data through input iterators
- many ways to consume data - return by value, lambda processing, return by pointer etc
- supports peeking elements by consumers
- allocator friendly

# TODO

- full clang support
- easy installation on target platforms
- implement slow consumer handling by allowing ring queue to grow dynamically - up to a limit. Also with this feature you'd get a pretty much unbounded queue as well and it would be very fast.
- benchmark against other implementations, including Java's Disruptor
- micro optimizations for optimal assembly output including relaxing some of the atomics
- test the queues on devices with weak memory ordering to catch any possible race condition
- use Relacy Race Detector to spot any potential race condition
- make publishers work with output iterators
- byte queue with variable message length
