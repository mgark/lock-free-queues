
# Features

Fairly generic lock-free templated lock-free implemenation for SPSC, MPMC, MPSC, SCMP. The idea is to provide the same interface for all of those queue and allow multiple complile-time options to tune them for real scenarios.
Also sequential publishing is supported for multi-producer scenario with a compile time flag.

It is pretty pretty fast. 

# TODO

- implement slow consumer handling by allowing ring queue to grow dynamically - up to a limit. Also with this feature you'd get pretty much unbounded queue as well and it would be very fast.
- benchmark against other implementations, including Java's Distruptor
- micro optimizations for optimal assembly output
