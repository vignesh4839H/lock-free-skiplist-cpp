#include <iostream>
#include <atomic>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <mutex>
#include <map>
#include <cassert>

constexpr int MAX_LEVEL = 16;
constexpr float PROBABILITY = 0.5f;
constexpr int MAX_THREADS = 32;

// Epoch-Based / Hazard Pointer Memory Reclamation Subsystem
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

// Concurrent Lock-Free Skip List
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

// Coarse Mutex Baseline
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

int main() {
    std::cout << "Running Lock-Free Skip List Suite..." << std::endl;
    LockFreeSkipList<int, int> list(-1, 1000000, -1);
    list.insert(10, 100, 0);
    std::cout << "Key 10 present: " << (list.contains(10, 0) ? "Yes" : "No") << std::endl;
    return 0;
}
