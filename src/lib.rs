use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

const MAX_LEVEL: usize = 16;
const PROBABILITY: f64 = 0.5;

pub struct Node<K, V> {
    pub key: K,
    pub value: V,
    pub height: usize,
    pub next: Vec<Atomic<Node<K, V>>>,
}

impl<K, V> Node<K, V> {
    pub fn new(key: K, value: V, height: usize) -> Self {
        let mut next = Vec::with_capacity(height);
        for _ in 0..height {
            next.push(Atomic::null());
        }
        Node { key, value, height, next }
    }
}

pub struct ConcurrentSkipList<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> ConcurrentSkipList<K, V>
where
    K: Ord + Default + Clone,
    V: Default + Clone,
{
    pub fn new() -> Self {
        let head_node = Node::new(K::default(), V::default(), MAX_LEVEL);
        ConcurrentSkipList {
            head: Atomic::new(head_node),
        }
    }

    fn random_level() -> usize {
        let mut rng = rand::thread_rng();
        let mut level = 1;
        while level < MAX_LEVEL && rng.gen::<f64>() < PROBABILITY {
            level += 1;
        }
        level
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        let height = Self::random_level();
        let new_node = Owned::new(Node::new(key.clone(), value, height)).into_shared(guard);

        loop {
            let mut preds = [Shared::null(); MAX_LEVEL];
            let mut succs = [Shared::null(); MAX_LEVEL];

            if self.find_position(&key, &mut preds, &mut succs, guard) {
                return false;
            }

            let node_ref = unsafe { new_node.deref() };
            for i in 0..height {
                node_ref.next[i].store(succs[i], Ordering::Relaxed);
            }

            let pred = if preds[0].is_null() {
                &self.head
            } else {
                let pred_ref = unsafe { preds[0].deref() };
                &pred_ref.next[0]
            };

            if pred
                .compare_exchange(succs[0], new_node, Ordering::Release, Ordering::Relaxed, guard)
                .is_ok()
            {
                for i in 1..height {
                    loop {
                        let pred_i = if preds[i].is_null() {
                            &self.head
                        } else {
                            let pred_ref = unsafe { preds[i].deref() };
                            &pred_ref.next[i]
                        };

                        node_ref.next[i].store(succs[i], Ordering::Relaxed);
                        if pred_i
                            .compare_exchange(succs[i], new_node, Ordering::Release, Ordering::Relaxed, guard)
                            .is_ok()
                        {
                            break;
                        }
                        self.find_position(&key, &mut preds, &mut succs, guard);
                    }
                }
                return true;
            }
        }
    }

    pub fn contains(&self, key: &K, guard: &Guard) -> bool {
        let mut curr = self.head.load(Ordering::Acquire, guard);
        while !curr.is_null() {
            let node = unsafe { curr.deref() };
            if node.key == *key {
                return true;
            }
            curr = node.next[0].load(Ordering::Acquire, guard);
        }
        false
    }

    fn find_position(
        &self,
        key: &K,
        preds: &mut [Shared<'_, Node<K, V>>; MAX_LEVEL],
        succs: &mut [Shared<'_, Node<K, V>>; MAX_LEVEL],
        guard: &Guard,
    ) -> bool {
        let mut pred = Shared::null();
        let mut curr = self.head.load(Ordering::Acquire, guard);

        for level in (0..MAX_LEVEL).rev() {
            while !curr.is_null() {
                let node = unsafe { curr.deref() };
                if node.key >= *key {
                    break;
                }
                pred = curr;
                curr = node.next[level].load(Ordering::Acquire, guard);
            }
            preds[level] = pred;
            succs[level] = curr;
        }

        if !curr.is_null() {
            let node = unsafe { curr.deref() };
            node.key == *key
        } else {
            false
        }
    }
}

pub struct MutexSkipList<K, V> {
    inner: Mutex<Vec<(K, V)>>,
}

impl<K: Ord + Clone, V: Clone> MutexSkipList<K, V> {
    pub fn new() -> Self {
        MutexSkipList {
            inner: Mutex::new(Vec::new()),
        }
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        let mut list = self.inner.lock().unwrap();
        if list.iter().any(|(k, _)| k == &key) {
            return false;
        }
        list.push((key, value));
        list.sort_by(|a, b| a.0.cmp(&b.0));
        true
    }

    pub fn contains(&self, key: &K) -> bool {
        let list = self.inner.lock().unwrap();
        list.iter().any(|(k, _)| k == key)
    }
}
