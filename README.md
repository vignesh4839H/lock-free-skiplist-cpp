# Lock-Free Concurrent Skip List in Rust

A high-performance, fully lock-free concurrent skip list implementation in Rust using Epoch-Based Memory Reclamation (`crossbeam-epoch`) and lock-free thread synchronization.

## Features
- **Lock-Free Concurrency**: Operates without coarse-grained locks or mutexes using atomic CAS operations.
- **Memory Safety & Safe Reclamation**: Leverages `crossbeam-epoch` to eliminate Use-After-Free (UAF) and ABA memory problems.
- **Benchmarking & Stress Test Suite**: Includes comparative throughput stress tests contrasting lock-free execution against mutex-based baselines across varying thread counts.

## Directory Structure
```text
lock-free-skiplist-rust/
├── Cargo.toml
├── README.md
└── src/
    ├── lib.rs
    └── main.rs
