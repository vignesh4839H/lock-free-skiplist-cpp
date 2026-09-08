#include "allocator.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <cassert>

MemoryPool<64, 100000> global_pool;

void test_worker(int thread_id) {
    std::vector<void*> ptrs;
    ptrs.reserve(1000);

    for (int i = 0; i < 1000; ++i) {
        void* p = ThreadLocalPool::alloc(global_pool);
        if (p) {
            ptrs.push_back(p);
        }
    }

    for (void* p : ptrs) {
        ThreadLocalPool::free(global_pool, p);
    }
}

void run_unit_tests() {
    std::cout << "[RUNNING] Unit tests for memory pool...\n";
    MemoryPool<32, 10> test_pool;
    
    void* p1 = test_pool.allocate();
    assert(p1 != nullptr);
    
    test_pool.deallocate(p1);
    void* p2 = test_pool.allocate();
    assert(p2 == p1);
    
    std::cout << "[PASSED] Basic allocation and deallocation verified.\n";
}

void run_benchmark() {
    std::cout << "\n=== Multi-threaded Allocator Benchmark ===\n";
    std::cout << "Threads | Custom Allocator (ops/sec) | Malloc (ops/sec)\n";
    std::cout << "-----------------------------------------------------\n";

    for (int num_threads : {1, 2, 4, 8, 16}) {
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> threads;

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back(test_worker, t);
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(end - start).count();
        double custom_ops = (num_threads * 2000) / elapsed;

        start = std::chrono::high_resolution_clock::now();
        threads.clear();

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&]() {
                std::vector<void*> m_ptrs;
                for (int i = 0; i < 1000; ++i) {
                    m_ptrs.push_back(std::malloc(64));
                }
                for (void* p : m_ptrs) {
                    std::free(p);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        end = std::chrono::high_resolution_clock::now();
        elapsed = std::chrono::duration<double>(end - start).count();
        double malloc_ops = (num_threads * 2000) / elapsed;

        std::cout << num_threads << "       | " << static_cast<long>(custom_ops) 
                  << "                 | " << static_cast<long>(malloc_ops) << "\n";
    }
}

int main() {
    run_unit_tests();
    run_benchmark();
    return 0;
}
