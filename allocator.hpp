#ifndef ALLOCATOR_HPP
#define ALLOCATOR_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <iostream>

struct Node {
    Node* next;
};

template <size_t BlockSize, size_t BlockCount>
class MemoryPool {
private:
    uint8_t* memory_slab;
    std::atomic<Node*> free_list;

public:
    MemoryPool() {
        memory_slab = static_cast<uint8_t*>(std::malloc(BlockSize * BlockCount));
        Node* first_node = reinterpret_cast<Node*>(memory_slab);
        
        Node* curr = first_node;
        for (size_t i = 0; i < BlockCount - 1; ++i) {
            Node* next_node = reinterpret_cast<Node*>(memory_slab + (i + 1) * BlockSize);
            curr->next = next_node;
            curr = next_node;
        }
        curr->next = nullptr;
        free_list.store(first_node, std::memory_order_relaxed);
    }

    ~MemoryPool() {
        std::free(memory_slab);
    }

    void* allocate() {
        Node* old_head = free_list.load(std::memory_order_acquire);
        while (old_head != nullptr) {
            Node* new_head = old_head->next;
            if (free_list.compare_exchange_weak(old_head, new_head, 
                                                std::memory_order_release, 
                                                std::memory_order_relaxed)) {
                return static_cast<void*>(old_head);
            }
        }
        return nullptr;
    }

    void deallocate(void* ptr) {
        if (!ptr) return;
        Node* node = static_cast<Node*>(ptr);
        Node* old_head = free_list.load(std::memory_order_relaxed);
        do {
            node->next = old_head;
        } while (!free_list.compare_exchange_weak(old_head, node, 
                                                  std::memory_order_release, 
                                                  std::memory_order_relaxed));
    }
};

class ThreadLocalPool {
public:
    static inline thread_local void* local_cache[16];
    static inline thread_local size_t cache_count = 0;

    template <typename Pool>
    static void* alloc(Pool& pool) {
        if (cache_count > 0) {
            return local_cache[--cache_count];
        }
        return pool.allocate();
    }

    template <typename Pool>
    static void free(Pool& pool, void* ptr) {
        if (cache_count < 16) {
            local_cache[cache_count++] = ptr;
        } else {
            pool.deallocate(ptr);
        }
    }
};

#endif
