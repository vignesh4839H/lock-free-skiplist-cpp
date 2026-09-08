#include <iostream>
#include <atomic>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <mutex>
#include <map>
#include <unordered_map>
#include <cassert>

constexpr int MAX_LEVEL = 16;
constexpr float PROBABILITY = 0.5f;
constexpr int MAX_THREADS = 32;

// Epoch-Based Memory Reclamation Subsystem
class EpochManager {
public:
    struct RetireNode {
        void* ptr;
        void (*deleter)(void*);
        uint64_t epoch;
    };

    static EpochManager& instance() {
        static EpochManager mgr;
        return mgr;
    }

    void enter_epoch(int thread_id) {
        active_epochs[thread_id].store(global_epoch.load(std::memory_order_relaxed), std::memory_order_seq_cst);
    }

    void exit_epoch(int thread_id) {
        active_epochs[thread_id].store(UINT64_MAX, std::memory_order_release);
    }

    template<typename T>
    void retire(T* ptr, int thread_id) {
        if (!ptr) return;
        uint64_t cur_epoch = global_epoch.load(std::memory_order_relaxed);
        retired_lists[thread_id].push_back({ptr, [](void* p) { delete static_cast<T*>(p); }, cur_epoch});
        
        if (retired_lists[thread_id].size() > 64) {
            reclaim(thread_id);
        }
    }

    void reclaim(int thread_id) {
        uint64_t min_epoch = global_epoch.load(std::memory_order_relaxed);
        for (int i = 0; i < MAX_THREADS; ++i) {
            uint64_t ep = active_epochs[i].load(std::memory_order_acquire);
            if (ep < min_epoch) min_epoch = ep;
        }

        auto& list = retired_lists[thread_id];
        auto it = list.begin();
        while (it != list.end()) {
            if (it->epoch < min_epoch) {
                it->deleter(it->ptr);
                it = list.erase(it);
            } else {
                ++it;
            }
        }
        global_epoch.fetch_add(1, std::memory_order_acq_rel);
    }

private:
    EpochManager() {
        for (int i = 0; i < MAX_THREADS; ++i) {
            active_epochs[i].store(UINT64_MAX);
        }
    }

    std::atomic<uint64_t> global_epoch{0};
    std::atomic<uint64_t> active_epochs[MAX_THREADS];
    std::vector<RetireNode> retired_lists[MAX_THREADS];
};

// Lock-Free Concurrent Skip List Implementation
template<typename Key, typename Value>
class LockFreeSkipList {
private:
    struct Node;

    struct MarkedPointer {
        Node* ptr;
        bool marked;

        MarkedPointer(Node* p = nullptr, bool m = false) : ptr(p), marked(m) {}

        bool operator==(const MarkedPointer& other) const {
            return ptr == other.ptr && marked == other.marked;
        }
    };

    struct Node {
        Key key;
        Value value;
        int height;
        std::atomic<MarkedPointer>* next;

        Node(Key k, Value val, int h) : key(k), value(val), height(h) {
            next = new std::atomic<MarkedPointer>[h];
            for (int i = 0; i < h; ++i) {
                next[i].store(MarkedPointer(nullptr, false), std::memory_order_relaxed);
            }
        }

        ~Node() {
            delete[] next;
        }
    };

    Node* head;
    Node* tail;

    int getRandomLevel() {
        thread_local std::mt19937 rng(std::random_device{}());
        thread_local std::uniform_real_distribution<float> dist(0.0f, 1.0f);
        int lvl = 1;
        while (dist(rng) < PROBABILITY && lvl < MAX_LEVEL) {
            lvl++;
        }
        return lvl;
    }

    bool findPosition(const Key& key, Node** preds, Node** succs, int thread_id) {
        bool marked = false;
        bool snip = false;
        Node* pred = nullptr;
        Node* curr = nullptr;
        Node* succ = nullptr;

    retry:
        while (true) {
            pred = head;
            for (int level = MAX_LEVEL - 1; level >= 0; --level) {
                curr = pred->next[level].load(std::memory_order_acquire).ptr;
                while (true) {
                    if (!curr) break;
                    
                    MarkedPointer succMarked = curr->next[level].load(std::memory_order_acquire);
                    succ = succMarked.ptr;
                    marked = succMarked.marked;

                    while (marked) {
                        MarkedPointer expected(curr, false);
                        MarkedPointer desired(succ, false);
                        snip = pred->next[level].compare_exchange_weak(
                            expected, desired,
                            std::memory_order_acq_rel, std::memory_order_relaxed
                        );
                        if (!snip) goto retry;

                        EpochManager::instance().retire(curr, thread_id);
                        curr = pred->next[level].load(std::memory_order_acquire).ptr;
                        if (!curr) break;
                        succMarked = curr->next[level].load(std::memory_order_acquire);
                        succ = succMarked.ptr;
                        marked = succMarked.marked;
                    }

                    if (curr != tail && curr->key < key) {
                        pred = curr;
                        curr = succ;
                    } else {
                        break;
                    }
                }
                preds[level] = pred;
                succs[level] = curr;
            }
            return (curr != tail && curr != nullptr && curr->key == key);
        }
    }

public:
    LockFreeSkipList(Key minKey, Key maxKey, Value defaultVal) {
        head = new Node(minKey, defaultVal, MAX_LEVEL);
        tail = new Node(maxKey, defaultVal, MAX_LEVEL);
        for (int i = 0; i < MAX_LEVEL; ++i) {
            head->next[i].store(MarkedPointer(tail, false), std::memory_order_relaxed);
        }
    }

    ~LockFreeSkipList() {
        Node* curr = head;
        while (curr != nullptr) {
            Node* nextNode = curr->next[0].load(std::memory_order_relaxed).ptr;
            delete curr;
            curr = nextNode;
        }
    }

    bool insert(Key key, Value val, int thread_id = 0) {
        EpochManager::instance().enter_epoch(thread_id);
        int topLevel = getRandomLevel();
        Node* preds[MAX_LEVEL];
        Node* succs[MAX_LEVEL];

        while (true) {
            bool found = findPosition(key, preds, succs, thread_id);
            if (found) {
                EpochManager::instance().exit_epoch(thread_id);
                return false;
            }

            Node* newNode = new Node(key, val, topLevel);
            for (int i = 0; i < topLevel; ++i) {
                newNode->next[i].store(MarkedPointer(succs[i], false), std::memory_order_relaxed);
            }

            Node* pred = preds[0];
            Node* succ = succs[0];

            MarkedPointer expected(succ, false);
            MarkedPointer desired(newNode, false);

            if (!pred->next[0].compare_exchange_weak(
                    expected, desired,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                delete newNode;
                continue;
            }

            for (int i = 1; i < topLevel; ++i) {
                while (true) {
                    pred = preds[i];
                    succ = succs[i];
                    newNode->next[i].store(MarkedPointer(succ, false), std::memory_order_relaxed);
                    
                    MarkedPointer exp(succ, false);
                    MarkedPointer des(newNode, false);
                    if (pred->next[i].compare_exchange_weak(
                            exp, des,
                            std::memory_order_acq_rel, std::memory_order_relaxed)) {
                        break;
                    }
                    findPosition(key, preds, succs, thread_id);
                }
            }
            EpochManager::instance().exit_epoch(thread_id);
            return true;
        }
    }

    bool remove(Key key, int thread_id = 0) {
        EpochManager::instance().enter_epoch(thread_id);
        Node* preds[MAX_LEVEL];
        Node* succs[MAX_LEVEL];

        while (true) {
            bool found = findPosition(key, preds, succs, thread_id);
            if (!found) {
                EpochManager::instance().exit_epoch(thread_id);
                return false;
            }

            Node* victim = succs[0];
            for (int level = victim->height - 1; level >= 1; --level) {
                MarkedPointer succMarked = victim->next[level].load(std::memory_order_acquire);
                while (!succMarked.marked) {
                    MarkedPointer desired(succMarked.ptr, true);
                    victim->next[level].compare_exchange_weak(
                        succMarked, desired,
                        std::memory_order_acq_rel, std::memory_order_relaxed);
                    succMarked = victim->next[level].load(std::memory_order_acquire);
                }
            }

            MarkedPointer succMarked = victim->next[0].load(std::memory_order_acquire);
            while (true) {
                bool iMarked = succMarked.marked;
                MarkedPointer desired(succMarked.ptr, true);
                bool success = victim->next[0].compare_exchange_weak(
                    succMarked, desired,
                    std::memory_order_acq_rel, std::memory_order_relaxed);
                succMarked = victim->next[0].load(std::memory_order_acquire);
                if (success) {
                    findPosition(key, preds, succs, thread_id);
                    EpochManager::instance().exit_epoch(thread_id);
                    return true;
                } else if (iMarked) {
                    EpochManager::instance().exit_epoch(thread_id);
                    return false;
                }
            }
        }
    }

    bool contains(Key key, int thread_id = 0) {
        EpochManager::instance().enter_epoch(thread_id);
        bool marked = false;
        Node* pred = head;
        Node* curr = nullptr;
        Node* succ = nullptr;

        for (int level = MAX_LEVEL - 1; level >= 0; --level) {
            curr = pred->next[level].load(std::memory_order_acquire).ptr;
            while (true) {
                if (!curr) break;
                MarkedPointer succMarked = curr->next[level].load(std::memory_order_acquire);
                succ = succMarked.ptr;
                marked = succMarked.marked;
                
                while (marked) {
                    curr = succ;
                    if (!curr) break;
                    succMarked = curr->next[level].load(std::memory_order_acquire);
                    succ = succMarked.ptr;
                    marked = succMarked.marked;
                }

                if (curr != tail && curr && curr->key < key) {
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }
        }
        EpochManager::instance().exit_epoch(thread_id);
        return (curr != tail && curr != nullptr && curr->key == key && !marked);
    }
};

// Coarse-Grained Mutex Locked Skip List Baseline
template<typename Key, typename Value>
class MutexSkipList {
private:
    struct Node {
        Key key;
        Value value;
        int height;
        Node** next;

        Node(Key k, Value val, int h) : key(k), value(val), height(h) {
            next = new Node*[h]();
        }

        ~Node() { delete[] next; }
    };

    Node* head;
    Node* tail;
    std::mutex mtx;

    int getRandomLevel() {
        thread_local std::mt19937 rng(std::random_device{}());
        thread_local std::uniform_real_distribution<float> dist(0.0f, 1.0f);
        int lvl = 1;
        while (dist(rng) < PROBABILITY && lvl < MAX_LEVEL) lvl++;
        return lvl;
    }

public:
    MutexSkipList(Key minKey, Key maxKey, Value defaultVal) {
        head = new Node(minKey, defaultVal, MAX_LEVEL);
        tail = new Node(maxKey, defaultVal, MAX_LEVEL);
        for (int i = 0; i < MAX_LEVEL; ++i) head->next[i] = tail;
    }

    ~MutexSkipList() {
        Node* curr = head;
        while (curr) {
            Node* nxt = curr->next[0];
            delete curr;
            curr = nxt;
        }
    }

    bool insert(Key key, Value val) {
        std::lock_guard<std::mutex> lock(mtx);
        Node* update[MAX_LEVEL];
        Node* curr = head;

        for (int i = MAX_LEVEL - 1; i >= 0; --i) {
            while (curr->next[i] && curr->next[i]->key < key) {
                curr = curr->next[i];
            }
            update[i] = curr;
        }

        curr = curr->next[0];
        if (curr && curr->key == key) return false;

        int lvl = getRandomLevel();
        Node* newNode = new Node(key, val, lvl);
        for (int i = 0; i < lvl; ++i) {
            newNode->next[i] = update[i]->next[i];
            update[i]->next[i] = newNode;
        }
        return true;
    }

    bool contains(Key key) {
        std::lock_guard<std::mutex> lock(mtx);
        Node* curr = head;
        for (int i = MAX_LEVEL - 1; i >= 0; --i) {
            while (curr->next[i] && curr->next[i]->key < key) {
                curr = curr->next[i];
            }
        }
        curr = curr->next[0];
        return (curr && curr->key == key);
    }
};

// Comprehensive Performance Analysis Benchmarking Suite
void runBenchmarkSuite() {
    std::cout << "=== Running Rigorous Skip List Benchmark Harness ===" << std::endl;
    const int threadCount = 8;
    const int opsPerThread = 15000;

    LockFreeSkipList<int, int> lfList(-1, 10000000, -1);
    MutexSkipList<int, int> mutexList(-1, 10000000, -1);
    
    std::map<int, int> stdMap;
    std::mutex stdMapMtx;

    // 1. Lock-Free Skip List Test
    auto startLF = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> lfThreads;
    for (int t = 0; t < threadCount; ++t) {
        lfThreads.emplace_back([&lfList, t, opsPerThread]() {
            for (int i = 0; i < opsPerThread; ++i) {
                int key = (t * opsPerThread) + i;
                lfList.insert(key, key * 5, t);
                lfList.contains(key, t);
                if (i % 4 == 0) lfList.remove(key, t);
            }
        });
    }
    for (auto& th : lfThreads) th.join();
    auto endLF = std::chrono::high_resolution_clock::now();
    double durLF = std::chrono::duration<double, std::milli>(endLF - startLF).count();

    // 2. Coarse Mutex Skip List Test
    auto startMutex = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> mutexThreads;
    for (int t = 0; t < threadCount; ++t) {
        mutexThreads.emplace_back([&mutexList, t, opsPerThread]() {
            for (int i = 0; i < opsPerThread; ++i) {
                int key = (t * opsPerThread) + i;
                mutexList.insert(key, key * 5);
                mutexList.contains(key);
            }
        });
    }
    for (auto& th : mutexThreads) th.join();
    auto endMutex = std::chrono::high_resolution_clock::now();
    double durMutex = std::chrono::duration<double, std::milli>(endMutex - startMutex).count();

    // 3. std::map with Mutex Test
    auto startMap = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> mapThreads;
    for (int t = 0; t < threadCount; ++t) {
        mapThreads.emplace_back([&stdMap, &stdMapMtx, t, opsPerThread]() {
            for (int i = 0; i < opsPerThread; ++i) {
                int key = (t * opsPerThread) + i;
                std::lock_guard<std::mutex> lock(stdMapMtx);
                stdMap[key] = key * 5;
                (void)stdMap.find(key);
            }
        });
    }
    for (auto& th : mapThreads) th.join();
    auto endMap = std::chrono::high_resolution_clock::now();
    double durMap = std::chrono::duration<double, std::milli>(endMap - startMap).count();

    std::cout << "Lock-Free Skip List Execution Time : " << durLF << " ms" << std::endl;
    std::cout << "Mutex Skip List Execution Time     : " << durMutex << " ms" << std::endl;
    std::cout << "std::map (Mutex-Locked) Time       : " << durMap << " ms" << std::endl;
    std::cout << "Speedup vs Coarse Mutex            : " << (durMutex / durLF) << "x" << std::endl;
}

int main() {
    runBenchmarkSuite();
    return 0;
}
