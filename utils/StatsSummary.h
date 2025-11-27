#pragma once
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>
#include <string>
#include <cstdint>
#include "metrics.h"

/**
 * @brief Aggregated percentile, average, and failure statistics
 *        computed from ThreadMetrics across all runs.
 */
struct StatsSummary {
    // enqueue
    uint64_t enq_avg = 0;
    uint64_t enq_p50 = 0;
    uint64_t enq_p95 = 0;
    uint64_t enq_p99 = 0;
    size_t   enq_count = 0;

    // dequeue
    uint64_t deq_avg = 0;
    uint64_t deq_p50 = 0;
    uint64_t deq_p95 = 0;
    uint64_t deq_p99 = 0;
    size_t   deq_count = 0;

    // enqueue failures
    double fail_avg_enq = 0.0;
    size_t fail_count_enq = 0;
    size_t failed_enqs = 0;

    // dequeue failures
    double fail_avg_deq = 0.0;
    size_t fail_count_deq = 0;
    size_t failed_deqs = 0;

    // lock wait
    uint64_t lock_avg = 0;
    uint64_t lock_p50 = 0;
    uint64_t lock_p95 = 0;
    uint64_t lock_p99 = 0;
    size_t   lock_count = 0;

    // critical section
    uint64_t crit_avg = 0;
    uint64_t crit_p50 = 0;
    uint64_t crit_p95 = 0;
    uint64_t crit_p99 = 0;
    size_t   crit_count = 0;

    // CAS
    uint64_t cas_attempts = 0;
    uint64_t cas_failures = 0;
};

/**
 * @brief Computes the p-th percentile of a vector of values.
 * @tparam T Numeric type.
 * @param v Input sample vector.
 * @param p Percentile in range [0,1].
 * @return Value at percentile p, or 0 if empty.
 */
template<typename T>
inline uint64_t percentile(const std::vector<T>& v, double p) {
    if (v.empty()) return 0;
    std::vector<T> copy = v;
    std::sort(copy.begin(), copy.end());
    size_t idx = static_cast<size_t>(p * copy.size());
    if (idx >= copy.size()) idx = copy.size() - 1;
    return copy[idx];
}

/**
 * @brief Converts raw ThreadMetrics into an aggregated StatsSummary.
 * @param m Completed thread metrics.
 * @return Aggregated summary containing averages, percentiles, and failure counts.
 */
inline StatsSummary computeStats(const ThreadMetrics& m) {
    StatsSummary s;

    s.enq_count = m.enq_latencies.size();
    if (s.enq_count) {
        uint64_t sum = 0;
        for (auto x : m.enq_latencies) sum += x;
        s.enq_avg = sum / s.enq_count;
        s.enq_p50 = percentile<uint64_t>(m.enq_latencies, 0.50);
        s.enq_p95 = percentile<uint64_t>(m.enq_latencies, 0.95);
        s.enq_p99 = percentile<uint64_t>(m.enq_latencies, 0.99);
    }

    s.deq_count = m.deq_latencies.size();
    if (s.deq_count) {
        uint64_t sum = 0;
        for (auto x : m.deq_latencies) sum += x;
        s.deq_avg = sum / s.deq_count;
        s.deq_p50 = percentile<uint64_t>(m.deq_latencies, 0.50);
        s.deq_p95 = percentile<uint64_t>(m.deq_latencies, 0.95);
        s.deq_p99 = percentile<uint64_t>(m.deq_latencies, 0.99);
    }

    s.fail_count_enq = m.enq_success.size();
    s.failed_enqs = m.failed_enqs;
    if (s.fail_count_enq) {
        uint64_t sum = 0;
        for (auto x : m.enq_success) sum += x;
        double success_rate = double(sum) / double(s.fail_count_enq);
        s.fail_avg_enq = (1.0 - success_rate) * 100.0;
    }

    s.fail_count_deq = m.deq_success.size();
    s.failed_deqs = m.failed_deqs;
    if (s.fail_count_deq) {
        uint64_t sum = 0;
        for (auto x : m.deq_success) sum += x;
        double success_rate = double(sum) / double(s.fail_count_deq);
        s.fail_avg_deq = (1.0 - success_rate) * 100.0;
    }

    s.lock_count = m.lock_waits.size();
    if (s.lock_count) {
        uint64_t sum = 0;
        for (auto x : m.lock_waits) sum += x;
        s.lock_avg = sum / s.lock_count;
        s.lock_p50 = percentile<uint64_t>(m.lock_waits, 0.50);
        s.lock_p95 = percentile<uint64_t>(m.lock_waits, 0.95);
        s.lock_p99 = percentile<uint64_t>(m.lock_waits, 0.99);
    }

    s.crit_count = m.crit_durations.size();
    if (s.crit_count) {
        uint64_t sum = 0;
        for (auto x : m.crit_durations) sum += x;
        s.crit_avg = sum / s.crit_count;
        s.crit_p50 = percentile<uint64_t>(m.crit_durations, 0.50);
        s.crit_p95 = percentile<uint64_t>(m.crit_durations, 0.95);
        s.crit_p99 = percentile<uint64_t>(m.crit_durations, 0.99);
    }

    s.cas_attempts = m.cas_attempts;
    s.cas_failures = m.cas_failures;

    return s;
}

/**
 * @brief Prints the StatsSummary to stdout in a human readable block format.
 * @param s Summary to print.
 */
inline void printStats(const StatsSummary& s) {
    std::cout << "\n=== Stats Summary ===\n";

    std::cout << "\n[enqueue latency ns]\n";
    std::cout << "avg "  << s.enq_avg  << "\n";
    std::cout << "p50 "  << s.enq_p50  << "\n";
    std::cout << "p95 "  << s.enq_p95  << "\n";
    std::cout << "p99 "  << s.enq_p99  << "\n";
    std::cout << "count " << s.enq_count << "\n";

    std::cout << "\n[dequeue latency ns]\n";
    std::cout << "avg "  << s.deq_avg  << "\n";
    std::cout << "p50 "  << s.deq_p50  << "\n";
    std::cout << "p95 "  << s.deq_p95  << "\n";
    std::cout << "p99 "  << s.deq_p99  << "\n";
    std::cout << "count " << s.deq_count << "\n";

    std::cout << "\n[enqueue failure stats]\n";
    std::cout << "total failures " << s.failed_enqs << "\n";
    std::cout << "avg failure rate " << s.fail_avg_enq << "%\n";
    std::cout << "count " << s.fail_count_enq << "\n";

    std::cout << "\n[dequeue failure stats]\n";
    std::cout << "total failures " << s.failed_deqs << "\n";
    std::cout << "avg failure rate " << s.fail_avg_deq << "%\n";
    std::cout << "count " << s.fail_count_deq << "\n";

    std::cout << "\n[lock wait ns]\n";
    std::cout << "avg "  << s.lock_avg  << "\n";
    std::cout << "p50 "  << s.lock_p50  << "\n";
    std::cout << "p95 "  << s.lock_p95  << "\n";
    std::cout << "p99 "  << s.lock_p99  << "\n";
    std::cout << "count " << s.lock_count << "\n";

    std::cout << "\n[critical section ns]\n";
    std::cout << "avg "  << s.crit_avg  << "\n";
    std::cout << "p50 "  << s.crit_p50  << "\n";
    std::cout << "p95 "  << s.crit_p95  << "\n";
    std::cout << "p99 "  << s.crit_p99  << "\n";
    std::cout << "count " << s.crit_count << "\n";

    std::cout << "\n[CAS]\n";
    std::cout << "attempts " << s.cas_attempts << "\n";
    std::cout << "failures " << s.cas_failures << "\n";
}

/**
 * @brief Appends a single benchmark result row to a CSV file.
 *
 * Creates the CSV header automatically if the file is empty.
 *
 * @param filename Output CSV file.
 * @param label Queue type label.
 * @param s Summary stats.
 * @param producers Producer thread count.
 * @param consumers Consumer thread count.
 * @param msgs_per_producer Messages produced per producer.
 * @param num_runs Number of workload repetitions.
 */
inline void write_csv_row(const std::string& filename,
                          const std::string& label,
                          const StatsSummary& s,
                          size_t producers,
                          size_t consumers,
                          size_t msgs_per_producer,
                          size_t num_runs)
{
    std::ofstream out(filename, std::ios::app);
    if (!out) return;

    out.seekp(0, std::ios::end);
    if (out.tellp() == 0) {
        out << "label,"
            << "producers,consumers,msgs_per_producer,num_runs,"
            << "enq_avg,enq_p50,enq_p95,enq_p99,enq_count,"
            << "deq_avg,deq_p50,deq_p95,deq_p99,deq_count,"
            << "lock_avg,lock_p50,lock_p95,lock_p99,lock_count,"
            << "crit_avg,crit_p50,crit_p95,crit_p99,crit_count,"
            << "cas_attempts,cas_failures,"
            << "failed_enqs,fail_avg_enq,"
            << "failed_deqs,fail_avg_deq\n";
    }

    out << label << ","
        << producers << ","
        << consumers << ","
        << msgs_per_producer << ","
        << num_runs << ","
        << s.enq_avg     << ","
        << s.enq_p50     << ","
        << s.enq_p95     << ","
        << s.enq_p99     << ","
        << s.enq_count   << ","
        << s.deq_avg     << ","
        << s.deq_p50     << ","
        << s.deq_p95     << ","
        << s.deq_p99     << ","
        << s.deq_count   << ","
        << s.lock_avg    << ","
        << s.lock_p50    << ","
        << s.lock_p95    << ","
        << s.lock_p99    << ","
        << s.lock_count  << ","
        << s.crit_avg    << ","
        << s.crit_p50    << ","
        << s.crit_p95    << ","
        << s.crit_p99    << ","
        << s.crit_count  << ","
        << s.cas_attempts << ","
        << s.cas_failures << ","
        << s.failed_enqs << ","
        << s.fail_avg_enq << ","
        << s.failed_deqs << ","
        << s.fail_avg_deq << ","
        << "\n";
}
