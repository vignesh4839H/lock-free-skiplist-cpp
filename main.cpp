#include <iostream>
#include <atomic>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <cassert>

constexpr int MAX_LEVEL = 16;
constexpr float PROBABILITY = 0.5f;

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
                next[i].store(MarkedPointer(nullptr, false));
            }
        }

        ~Node() {
            delete[] next;
        }
    };

    Node* head;
    Node* tail;

    int getRandomLevel() {
        static thread_local std::mt19937 rng(std::random_device{}());
        static thread_local std::uniform_real_distribution<float> dist(0.0f, 1.0f);
        int lvl = 1;
        while (dist(rng) < PROBABILITY && lvl < MAX_LEVEL) {
            lvl++;
        }
        return lvl;
    }

    bool findPosition(const Key& key, Node** preds, Node** succs) {
        bool marked = false;
        bool snip = false;
        Node* pred = nullptr;
        Node* curr = nullptr;
        Node* succ = nullptr;

    retry:
        while (true) {
            pred = head;
            for (int level = MAX_LEVEL - 1; level >= 0; --level) {
                curr = pred->next[level].load().ptr;
                while (true) {
                    if (!curr) break;
                    
                    MarkedPointer succMarked = curr->next[level].load();
                    succ = succMarked.ptr;
                    marked = succMarked.marked;

                    while (marked) {
                        MarkedPointer expected(curr, false);
                        MarkedPointer desired(succ, false);
                        snip = pred->next[level].compare_exchange_weak(expected, desired);
                        if (!snip) goto retry;
                        curr = pred->next[level].load().ptr;
                        if (!curr) break;
                        succMarked = curr->next[level].load();
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
            head->next[i].store(MarkedPointer(tail, false));
        }
    }

    ~LockFreeSkipList() {
        Node* curr = head;
        while (curr != nullptr) {
            Node* nextNode = curr->next[0].load().ptr;
            delete curr;
            curr = nextNode;
        }
    }

    bool insert(Key key, Value val) {
        int topLevel = getRandomLevel();
        Node* preds[MAX_LEVEL];
        Node* succs[MAX_LEVEL];

        while (true) {
            bool found = findPosition(key, preds, succs);
            if (found) {
                return false;
            }

            Node* newNode = new Node(key, val, topLevel);
            for (int i = 0; i < topLevel; ++i) {
                newNode->next[i].store(MarkedPointer(succs[i], false));
            }

            Node* pred = preds[0];
            Node* succ = succs[0];

            MarkedPointer expected(succ, false);
            MarkedPointer desired(newNode, false);

            if (!pred->next[0].compare_exchange_weak(expected, desired)) {
                delete newNode;
                continue;
            }

            for (int i = 1; i < topLevel; ++i) {
                while (true) {
                    pred = preds[i];
                    succ = succs[i];
                    newNode->next[i].store(MarkedPointer(succ, false));
                    
                    MarkedPointer exp(succ, false);
                    MarkedPointer des(newNode, false);
                    if (pred->next[i].compare_exchange_weak(exp, des)) {
                        break;
                    }
                    findPosition(key, preds, succs);
                }
            }
            return true;
        }
    }

    bool remove(Key key) {
        Node* preds[MAX_LEVEL];
        Node* succs[MAX_LEVEL];
        Node* victim = nullptr;

        while (true) {
            bool found = findPosition(key, preds, succs);
            if (!found) {
                return false;
            }

            victim = succs[0];
            for (int level = victim->height - 1; level >= 1; --level) {
                MarkedPointer succMarked = victim->next[level].load();
                while (!succMarked.marked) {
                    MarkedPointer desired(succMarked.ptr, true);
                    victim->next[level].compare_exchange_weak(succMarked, desired);
                    succMarked = victim->next[level].load();
                }
            }

            MarkedPointer succMarked = victim->next[0].load();
            while (true) {
                bool iMarked = succMarked.marked;
                MarkedPointer desired(succMarked.ptr, true);
                bool success = victim->next[0].compare_exchange_weak(succMarked, desired);
                succMarked = victim->next[0].load();
                if (success) {
                    findPosition(key, preds, succs);
                    return true;
                } else if (iMarked) {
                    return false;
                }
            }
        }
    }

    bool contains(Key key) {
        bool marked = false;
        Node* pred = head;
        Node* curr = nullptr;
        Node* succ = nullptr;

        for (int level = MAX_LEVEL - 1; level >= 0; --level) {
            curr = pred->next[level].load().ptr;
            while (true) {
                if (!curr) break;
                MarkedPointer succMarked = curr->next[level].load();
                succ = succMarked.ptr;
                marked = succMarked.marked;
                
                while (marked) {
                    curr = succ;
                    if (!curr) break;
                    succMarked = curr->next[level].load();
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
        return (curr != tail && curr != nullptr && curr->key == key && !marked);
    }
};

void runConcurrencyTest() {
    std::cout << "[INFO] Initializing Lock-Free Skip List Concurrency Benchmarks..." << std::endl;
    LockFreeSkipList<int, int> skipList(-1, 1000000, -1);

    const int numThreads = 8;
    const int opsPerThread = 5000;
    std::vector<std::thread> workers;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numThreads; ++i) {
        workers.emplace_back([&skipList, i, opsPerThread]() {
            for (int j = 0; j < opsPerThread; ++j) {
                int key = (i * opsPerThread) + j;
                skipList.insert(key, key * 10);
                skipList.contains(key);
                if (j % 2 == 0) {
                    skipList.remove(key);
                }
            }
        });
    }

    for (auto& w : workers) {
        w.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;

    std::cout << "[SUCCESS] Completed " << (numThreads * opsPerThread * 3) 
              << " concurrent ops in " << duration.count() << " ms." << std::endl;
}

int main() {
    runConcurrencyTest();
    return 0;
}
