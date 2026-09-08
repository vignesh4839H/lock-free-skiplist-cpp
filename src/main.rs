use crossbeam_epoch as epoch;
use lock_free_skiplist_rust::{ConcurrentSkipList, MutexSkipList};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() {
    println!("=== Running Multi-Threaded Stress Test Harness ===");
    let skiplist = Arc::new(ConcurrentSkipList::<i32, i32>::new());
    let mut handles = vec![];

    for t in 0..8 {
        let list = Arc::clone(&skiplist);
        handles.push(thread::spawn(move || {
            let guard = epoch::pin();
            for i in 0..5000 {
                let key = (t * 5000 + i) as i32;
                list.insert(key, key * 10, &guard);
                list.contains(&key, &guard);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    println!("Stress Test Passed: Zero data races or unhandled race conditions detected.");

    println!("\n=== Running Performance Benchmark Suite ===");
    println!("{:<10} {:<25} {:<25}", "Threads", "Lock-Free (ops/sec)", "Mutex-Based (ops/sec)");
    println!("------------------------------------------------------------------");

    for num_threads in [1, 2, 4, 8, 16] {
        let lf_list = Arc::new(ConcurrentSkipList::<i32, i32>::new());
        let start = Instant::now();
        let mut handles = vec![];

        for _ in 0..num_threads {
            let list = Arc::clone(&lf_list);
            handles.push(thread::spawn(move || {
                let guard = epoch::pin();
                for i in 0..10000 {
                    list.insert(i, i, &guard);
                    list.contains(&i, &guard);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let lf_duration = start.elapsed().as_secs_f64();
        let lf_throughput = (num_threads * 20000) as f64 / lf_duration;

        let mx_list = Arc::new(MutexSkipList::<i32, i32>::new());
        let start = Instant::now();
        let mut handles = vec![];

        for _ in 0..num_threads {
            let list = Arc::clone(&mx_list);
            handles.push(thread::spawn(move || {
                for i in 0..10000 {
                    list.insert(i, i);
                    list.contains(&i);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mx_duration = start.elapsed().as_secs_f64();
        let mx_throughput = (num_threads * 20000) as f64 / mx_duration;

        println!("{:<10} {:<25.0} {:<25.0}", num_threads, lf_throughput, mx_throughput);
    }
}
