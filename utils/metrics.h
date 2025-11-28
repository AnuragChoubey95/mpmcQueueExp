#pragma once
#include <cstdint>
#include <vector>

/**
 * @brief Per-thread metrics collected during queue operations.
 *
 * Tracks enqueue and dequeue counts, failures, timing samples,
 * lock-wait durations, critical-section durations, CAS metadata,
 * and per-operation success/failure indicators.
 */
struct ThreadMetrics {

    /** @brief Aggregate counters for operations */
    uint64_t enq_count = 0;
    uint64_t deq_count = 0;
    uint64_t failed_enqs = 0;
    uint64_t failed_deqs = 0;
    uint64_t cas_attempts = 0;
    uint64_t cas_failures = 0;

    /** @brief Latency and diagnostic samples for percentiles and analysis */
    std::vector<uint64_t> enq_latencies;
    std::vector<uint64_t> deq_latencies;
    std::vector<uint64_t> lock_waits;
    std::vector<uint64_t> crit_durations;
    std::vector<uint64_t> cas_retry_counts;
    std::vector<uint8_t>  enq_success;
    std::vector<uint8_t>  deq_success;

    /**
     * @brief Merge metrics from another ThreadMetrics into this structure.
     * @param other Source metrics to accumulate.
     */
    void appendFrom(const ThreadMetrics& other) {
        enq_count    += other.enq_count;
        deq_count    += other.deq_count;
        failed_enqs  += other.failed_enqs;
        failed_deqs  += other.failed_deqs;
        cas_attempts += other.cas_attempts;
        cas_failures += other.cas_failures;

        enq_latencies.insert(enq_latencies.end(),
                             other.enq_latencies.begin(),
                             other.enq_latencies.end());

        deq_latencies.insert(deq_latencies.end(),
                             other.deq_latencies.begin(),
                             other.deq_latencies.end());

        lock_waits.insert(lock_waits.end(),
                          other.lock_waits.begin(),
                          other.lock_waits.end());

        crit_durations.insert(crit_durations.end(),
                              other.crit_durations.begin(),
                              other.crit_durations.end());

        cas_retry_counts.insert(cas_retry_counts.end(),
                                other.cas_retry_counts.begin(),
                                other.cas_retry_counts.end());

        enq_success.insert(enq_success.end(),
                           other.enq_success.begin(),
                           other.enq_success.end());

        deq_success.insert(deq_success.end(),
                           other.deq_success.begin(),
                           other.deq_success.end());
    }

    /**
     * @brief Reset all counters and sample vectors.
     * Resets the structure for reuse in the next benchmark iteration.
     */
    void reset() {
        enq_count    = 0;
        deq_count    = 0;
        failed_enqs  = 0;
        failed_deqs  = 0;
        cas_attempts = 0;
        cas_failures = 0;

        enq_latencies.clear();
        deq_latencies.clear();
        enq_success.clear();
        deq_success.clear();
        lock_waits.clear();
        crit_durations.clear();
        cas_retry_counts.clear();
    }
};


/**
 * @brief Static instrumentation helpers for recording timing and event data.
 *
 * These functions update a ThreadMetrics instance with per-operation timing,
 * success/failure outcomes, CAS metadata, and lock/critical-section timing.
 */
struct Instrumentation {

    /**
     * @brief Record lock-wait duration.
     * @param m  Thread metrics.
     * @param ns Nanoseconds spent waiting for lock.
     */
    static inline void on_lock_wait(ThreadMetrics& m, uint64_t ns) {
        m.lock_waits.push_back(ns);
    }

    /**
     * @brief Record critical-section duration.
     * @param m  Thread metrics.
     * @param ns Nanoseconds spent inside critical region.
     */
    static inline void on_critical_section(ThreadMetrics& m, uint64_t ns) {
        m.crit_durations.push_back(ns);
    }

    /**
     * @brief Record an enqueue attempt, its latency, and outcome.
     * @param m    Thread metrics.
     * @param ns   Latency in nanoseconds.
     * @param succ True if enqueue succeeded.
     */
    static inline void on_enqueue_latency(ThreadMetrics& m, uint64_t ns, bool succ) {
        m.enq_count++;
        if (succ) m.enq_latencies.push_back(ns);
        m.enq_success.push_back(succ ? 1 : 0);
        if (!succ) m.failed_enqs++;
    }

    /**
     * @brief Record a dequeue attempt, its latency, and outcome.
     * @param m    Thread metrics.
     * @param ns   Latency in nanoseconds.
     * @param succ True if dequeue succeeded.
     */
    static inline void on_dequeue_latency(ThreadMetrics& m, uint64_t ns, bool succ) {
        m.deq_count++;
        if (succ) m.deq_latencies.push_back(ns);
        m.deq_success.push_back(succ ? 1 : 0);
        if (!succ) m.failed_deqs++;
    }

    /**
     * @brief Record a CAS attempt.
     * @param m Thread metrics.
     */
    static inline void on_cas_attempt(ThreadMetrics& m) {
        m.cas_attempts++;
    }

    /**
     * @brief Record a CAS failure.
     * @param m Thread metrics.
     */
    static inline void on_cas_fail(ThreadMetrics& m) {
        m.cas_failures++;
    }

    /**
     * @brief Retrieve the current time in nanoseconds.
     * @return Monotonic timestamp suitable for latency measurement.
     */
    static uint64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }
};
