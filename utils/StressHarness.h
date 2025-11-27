#pragma once
#include <vector>
#include <thread>
#include <chrono>
#include "metrics.h"


/**
 * @brief Runs a fixed producer and consumer workload against a queue.
 * Each worker thread updates its own metrics slot without locking.
 */
template <typename QueueType>
class StressHarness {
public:
    /**
     * @brief Initializes harness with producer and consumer counts and messages per producer.
     */
    StressHarness(size_t p, size_t c, size_t msgs, size_t num_runs_)
        : producers(p),
          consumers(c),
          msgs_per_producer(msgs),
          num_runs(num_runs_)
    {
        size_t total_threads = producers + consumers;
        per_thread_metrics.resize(total_threads);
    }

    /**
     * @brief Runs the workload on the given queue instance and aggregates metrics.
     */
    void run(QueueType& queue) {
        for (int i = 0; i < num_runs; ++i){
            spawn_consumers(queue);
            spawn_producers(queue);
            
            join_producers();
            join_consumers();

            aggregate_thread_metrics();

            producer_threads.clear();
            consumer_threads.clear();

        }
    }


    /** @brief Final aggregated metrics for all runs */
    ThreadMetrics final_metrics;

private:
    /**
     * @brief Spawns producer threads and assigns each a metrics slot.
     */
    void spawn_producers(QueueType& q) {
        for (size_t i = 0; i < producers; i++) {
            size_t slot = i;

            producer_threads.emplace_back([&, slot, worker_id = i]{
                uint8_t msg[BLOCK_SIZE] = {0};
                ThreadMetrics& local = per_thread_metrics[slot];

                for (size_t n = 0; n < msgs_per_producer; n++) {
                    uint64_t t0 = Instrumentation:: Instrumentation::now_ns();
                    bool succ = q.enqueue(msg, local, worker_id);
                    uint64_t t1 = Instrumentation:: Instrumentation::now_ns();

                    Instrumentation::on_enqueue_latency(local, t1 - t0, succ);
                }
            });
        }
    }

    /**
     * @brief Spawns consumer threads and assigns each a metrics slot.
     */
    void spawn_consumers(QueueType& q) {
        for (size_t i = 0; i < consumers; i++) {
            size_t slot = producers + i;

            consumer_threads.emplace_back([&, slot, worker_id = i]{
                uint8_t msg[BLOCK_SIZE];
                ThreadMetrics& local = per_thread_metrics[slot];
                for (size_t n = 0; n < (msgs_per_producer * producers)/ consumers; n++) {
                    uint64_t t0 = Instrumentation:: Instrumentation::now_ns();
                    bool succ = q.dequeue(msg, local, worker_id);
                    uint64_t t1 = Instrumentation:: Instrumentation::now_ns();

                    Instrumentation::on_dequeue_latency(local, t1 - t0, succ);
                }
            });
        }
    }

    /** @brief Waits for all producer threads to complete */
    void join_producers() {
        for (auto& t : producer_threads) {
            t.join();
        }
    }

    /** @brief Waits for all consumer threads to complete */
    void join_consumers() {
        for (auto& t : consumer_threads) {
            t.join();
        }
    }

    /** @brief Aggregates metrics from all threads into final_metrics and prints statistics */
    void aggregate_thread_metrics() {
        // Merge stats
        for (auto& tm : per_thread_metrics) {
           final_metrics.appendFrom(tm);
           tm.reset();
        }        
    }

private:
    /** @brief Number of producer threads */
    size_t producers;

    /** @brief Number of consumer threads */
    size_t consumers;

    /** @brief Messages each producer must send */
    size_t msgs_per_producer;

    /** @brief The number of runs for which the workload is simulated */
    size_t num_runs;    

    /** @brief Per thread metrics slots, one per worker thread */
    std::vector<ThreadMetrics> per_thread_metrics;

    /** @brief Producer worker threads */
    std::vector<std::thread> producer_threads;

    /** @brief Consumer worker threads */
    std::vector<std::thread> consumer_threads;
    
};
