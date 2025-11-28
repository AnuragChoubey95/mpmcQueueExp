#pragma once
#include <cstdint>
#include <cstring>
#include <atomic>
#include <thread>
#include "slot.h"
#include "../utils/metrics.h"

#define NUM_SHARDS 4

/**
 * @brief Sharded queue using two spinlocks per shard.
 *
 * Each shard has its own:
 *   - head index + head spinlock
 *   - tail index + tail spinlock
 *
 * Producers and consumers map to shards by worker_id % NUM_SHARDS.
 */
class MessageQueueSharded {
public:
    MessageQueueSharded(size_t total_capacity = CAPACITY) {
        size_t per = total_capacity / NUM_SHARDS;
        if (per == 0) per = 1;

        shard_capacity = per;

        slots = new Slot[NUM_SHARDS * shard_capacity];

        head_index.resize(NUM_SHARDS, 0);
        tail_index.resize(NUM_SHARDS, 0);
        full_flag.resize(NUM_SHARDS, false);
        count.resize(NUM_SHARDS, 0);
    }

    ~MessageQueueSharded() {
        delete[] slots;
    }

    // ------------------------------------------------------------
    // ENQUEUE into shard via spinlock pair
    // ------------------------------------------------------------
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        size_t shard = worker_id % NUM_SHARDS;
        size_t base = shard * shard_capacity;

        // ----------------------
        // Acquire TAIL LOCK
        // ----------------------
        uint64_t wait_start = Instrumentation::now_ns();
        tail_lock[shard].lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (full_flag[shard]) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            tail_lock[shard].unlock();
            return false;
        }

        size_t t = tail_index[shard];
        std::memcpy(slots[base + t].data, data, BLOCK_SIZE);

        t = (t + 1) % shard_capacity;
        tail_index[shard] = t;
        count[shard]++;

        if (t == head_index[shard]) {
            full_flag[shard] = true;
        }

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        tail_lock[shard].unlock();
        return true;
    }

    // ------------------------------------------------------------
    // DEQUEUE via spinlock head-lock
    // ------------------------------------------------------------
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        size_t shard = worker_id % NUM_SHARDS;
        size_t base = shard * shard_capacity;

        // ----------------------
        // Acquire HEAD LOCK
        // ----------------------
        uint64_t wait_start = Instrumentation::now_ns();
        head_lock[shard].lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (!full_flag[shard] &&
            head_index[shard] == tail_index[shard]) {

            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            head_lock[shard].unlock();
            return false;
        }

        size_t h = head_index[shard];

        std::memcpy(out_data, slots[base + h].data, BLOCK_SIZE);

        h = (h + 1) % shard_capacity;
        head_index[shard] = h;
        count[shard]--;
        full_flag[shard] = false;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        head_lock[shard].unlock();
        return true;
    }

private:
    Slot* slots;

    size_t shard_capacity;

    // Per-shard ring metadata
    std::vector<size_t> head_index;
    std::vector<size_t> tail_index;
    std::vector<bool> full_flag;
    std::vector<size_t> count;

    // Two spinlocks per shard
    SpinLock head_lock[NUM_SHARDS];
    SpinLock tail_lock[NUM_SHARDS];
};
