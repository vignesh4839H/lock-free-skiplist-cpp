# Lock-Free Concurrent Skip List in C++

A high-performance concurrent lock-free skip list implementation in C++ featuring Epoch-Based Memory Reclamation (EBR) and explicit atomic memory ordering.

## Features
- **Safe Memory Reclamation**: Integrates Epoch-Based Reclamation (EBR) to prevent memory leaks and race conditions.
- **Atomic Memory Ordering**: Uses C++ atomic operations for high thread-safety without global locks.
- **Lock-Free Operations**: High throughput concurrent insertion and lookup.
