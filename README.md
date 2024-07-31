
# Introduction

Fairly generic header-only and slot based (byte ones will come as well) lock-free implemenation for **multicast**(**anycast** will come soon as well) SPSC, MPMC, MPSC, SCMP. Note that in case of multi consumers, all of them would get a copy of data.  The idea is to provide the same interface for all of those queues and allow multiple compile-time options to tune them for real scenarios.  Also Synchronized publishing is supported for multi-producer scenarios with a compile time setting.

# Design

## Reliable queues

The idea behind the implementation is pretty simple - just have a fixed number of slots in a ring buffer with fixed size, and don't let consumers touch the memory from the ring buffer. Instead let consumers only update their corresponding read index at intervals (default is each 25% of the ring buffer size). The producer would make sure not to override data by comparing the index it writes to  the min consumer committed read idx.

## Conflated queues [x86 only]

Consumers don't need to track their index even, just check if a given slot got a newer version, copy it and after the copy has been finished make sure the version has not changed as the publisher might have warped around by the time the copy finished. It requires using some memory fences as well (hardware no-op on x86) and odd / even trick as well which is pretty much seq lock, but it scales very well and it is super fast.

# Requirements

- at least C++20
- Catch2 for tests
- CMake for building

# Features

- reliable (if there is at least one consumer live, publishers won't be able to override the data and would basically stop progress until there is space on the ring) and conflated queue types (publishers can override data in case consumers are slow)
- lock-free / wait-free. There is only one tiny usage of spin-locks on the slow path
- dynamic consumers attach / detach
- single producer / consumer interface for all kinds of queues
- Synchronized mutti-publishers supported
- it is pretty pretty fast.
- multiple compile-time tweaks
- supports reading data through input iterators
- many ways to consume data - return by value, lambda processing, consume by copying through the input object refrence 
- supports peeking elements by consumers
- allocator friendly

# TODO

- full clang support
- byte queue with variable message length
- ~right now consumers can attach and detach dynamically, but so shall be producers too!~
- easy installation on target platforms
- write more tests
- ~[anycast queues] provide an option for multi consumers to exclusively consume items from the queue, which is more like a traditional queue.~
- ~implement slow consumer handling by allowing ring queue to grow dynamically - up to a limit. Also with this feature you'd get a pretty much unbounded queue as well and it would be very fast.~ [Added support for SPMC queues only, synchronized MPMC queues not available]
- benchmark against other implementations, including Java's Disruptor
- support for queues over shared memory
- micro optimizations for optimal assembly output including relaxing some of the atomics
- test the queues on devices with weak memory ordering to catch any possible race condition
- use Relacy Race Detector to spot any potential race condition
- make publishers work with output iterators
