#pragma once
#include <cstdint>
#include <cstring>
#include <mutex>
#include <thread>
#include <functional>
#include "slot.h"
#include "../utils/metrics.h"

#define NUM_SHARDS 4

/**
 * @brief Sharded two-lock message queue.
 *
 * Partitions a fixed-capacity ring buffer into NUM_SHARDS independent
 * sub-queues. Producer and consumer threads are mapped to shards via
 * worker_id mod NUM_SHARDS. Each shard maintains its own head/tail
 * pointers and lock pair, reducing contention relative to a single
 * global queue.
 *
 * Enqueue/dequeue operations return false when the selected shard is
 * full or empty. Latency, lock-wait, and critical-section durations
 * are recorded through Instrumentation.
 */
class MessageQueueSharded {
public:
    /**
     * @brief Construct a sharded queue with an overall capacity.
     *
     * Total capacity is divided evenly across NUM_SHARDS. Each shard
     * receives at least one slot regardless of total size.
     *
     * @param total_capacity Total number of elements across all shards.
     */
    MessageQueueSharded(size_t total_capacity = CAPACITY)
        : total_cap(total_capacity)
    {
        if (total_cap == 0)
            total_cap = 1;

        cap = total_cap / NUM_SHARDS;
        if (cap == 0)
            cap = 1;

        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            shards[i].slots = new Slot[cap];
            shards[i].head_index = 0;
            shards[i].tail_index = 0;
            shards[i].full_flag = false;
        }
    }

    /**
     * @brief Destroy the sharded queue and release per-shard buffers.
     */
    ~MessageQueueSharded() {
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            delete[] shards[i].slots;
        }
    }

    /**
     * @brief Enqueue a message into the shard selected by worker_id.
     *
     * Locks the shard’s tail mutex, checks for space, writes the data,
     * updates the tail pointer, and records timing/metrics.
     *
     * @param data Pointer to BLOCK_SIZE bytes.
     * @param local Thread-local metrics.
     * @param worker_id Worker index used to select a shard.
     * @return true on success, false if shard is full or data is null.
     */
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        size_t idx = worker_id % NUM_SHARDS;
        Shard& s = shards[idx];

        uint64_t wait_start = Instrumentation::now_ns();
        s.tail_lock.lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (s.full_flag) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            s.tail_lock.unlock();
            return false;
        }

        std::memcpy(s.slots[s.tail_index].data, data, BLOCK_SIZE);
        s.tail_index = (s.tail_index + 1) % cap;

        if (s.tail_index == s.head_index)
            s.full_flag = true;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        s.tail_lock.unlock();
        return true;
    }

    /**
     * @brief Dequeue a message from the shard selected by worker_id.
     *
     * Locks the shard’s head mutex, checks for availability, copies
     * data to out_data, updates the head pointer, and records metrics.
     *
     * @param out_data Output buffer for BLOCK_SIZE bytes.
     * @param local Thread-local metrics.
     * @param worker_id Worker index used to select a shard.
     * @return true on success, false if shard is empty or buffer null.
     */
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        size_t idx = worker_id % NUM_SHARDS;
        Shard& s = shards[idx];

        uint64_t wait_start = Instrumentation::now_ns();
        s.head_lock.lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (!s.full_flag && s.head_index == s.tail_index) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            s.head_lock.unlock();
            return false;
        }

        std::memcpy(out_data, s.slots[s.head_index].data, BLOCK_SIZE);
        s.head_index = (s.head_index + 1) % cap;
        s.full_flag = false;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        s.head_lock.unlock();
        return true;
    }

private:
    /**
     * @brief Internal structure representing a single shard.
     *
     * Each shard holds its own slot buffer, head/tail indices,
     * full/empty flag, and independent lock pair.
     */
    struct Shard {
        Slot* slots = nullptr;
        size_t head_index = 0;
        size_t tail_index = 0;
        bool full_flag = false;
        std::mutex head_lock;
        std::mutex tail_lock;
    };

    size_t total_cap;
    size_t cap;
    Shard shards[NUM_SHARDS];
};
