#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>
#include <thread>
#include "slot.h"
#include "../utils/metrics.h"

#define NUM_SHARDS 4

/**
 * @brief Lock free sharded message queue using per shard atomics.
 *
 * Total capacity is divided across NUM_SHARDS independent shards.
 * Each shard implements a sequence based ring buffer with atomic head
 * and tail indices. Producers and consumers are routed to shards using
 * worker_id modulo NUM_SHARDS. Operations are non blocking and use
 * CAS loops, yielding on contention.
 */
class MessageQueueAtomicSharded {
public:
    /**
     * @brief Construct a sharded atomic queue with a total capacity.
     *
     * The total capacity is split evenly across NUM_SHARDS. Each shard
     * receives at least one slot even if total_capacity is small.
     *
     * @param total_capacity Total number of elements across all shards.
     */
    explicit MessageQueueAtomicSharded(size_t total_capacity = CAPACITY)
        : total_cap(total_capacity)
    {
        if (total_cap == 0) total_cap = 1;

        cap = total_cap / NUM_SHARDS;
        if (cap == 0) cap = 1;

        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            shards[i].slots = new Slot[cap];
            shards[i].head.store(0, std::memory_order_relaxed);
            shards[i].tail.store(0, std::memory_order_relaxed);

            for (size_t j = 0; j < cap; ++j) {
                shards[i].slots[j].seq.store(j, std::memory_order_relaxed);
            }
        }
    }

    /**
     * @brief Destroy the queue and release per shard storage.
     */
    ~MessageQueueAtomicSharded() {
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            delete[] shards[i].slots;
        }
    }

    /**
     * @brief Enqueue a message into the shard selected by worker_id.
     *
     * Uses a sequence based ring algorithm. If the target shard is
     * full the call returns false. On contention the CAS loop retries
     * and yields to other threads. CAS attempts and failures are
     * recorded in the provided metrics.
     *
     * @param data Pointer to BLOCK_SIZE bytes to enqueue.
     * @param local Thread local metrics.
     * @param worker_id Worker index used to choose a shard.
     * @return true on success, false if the shard is full or data is null.
     */
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        size_t idx = worker_id % NUM_SHARDS;
        Shard& s = shards[idx];

        while (true) {
            size_t pos = s.tail.load(std::memory_order_relaxed);
            Slot& slot = s.slots[pos % cap];

            size_t seq = slot.seq.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (diff == 0) {
                Instrumentation::on_cas_attempt(local);
                if (!s.tail.compare_exchange_weak(
                        pos, pos + 1,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed))
                {
                    Instrumentation::on_cas_fail(local);
                    continue;
                }

                std::memcpy(slot.data, data, BLOCK_SIZE);
                slot.seq.store(pos + 1, std::memory_order_release);
                return true;
            } else if (diff < 0) {
                return false;
            } else {
                std::this_thread::yield();
            }
        }
    }

    /**
     * @brief Dequeue a message from the shard selected by worker_id.
     *
     * Uses the same sequence based protocol as enqueue. If the shard
     * is empty the call returns false. On contention CAS is retried
     * and the thread yields. CAS metadata is recorded in metrics.
     *
     * @param out_data Output buffer for BLOCK_SIZE bytes.
     * @param local Thread local metrics.
     * @param worker_id Worker index used to choose a shard.
     * @return true on success, false if the shard is empty or buffer is null.
     */
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        size_t idx = worker_id % NUM_SHARDS;
        Shard& s = shards[idx];

        while (true) {
            size_t pos = s.head.load(std::memory_order_relaxed);
            Slot& slot = s.slots[pos % cap];

            size_t seq = slot.seq.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (diff == 0) {
                Instrumentation::on_cas_attempt(local);
                if (!s.head.compare_exchange_weak(
                        pos, pos + 1,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed))
                {
                    Instrumentation::on_cas_fail(local);
                    continue;
                }

                std::memcpy(out_data, slot.data, BLOCK_SIZE);
                slot.seq.store(pos + cap, std::memory_order_release);
                return true;
            } else if (diff < 0) {
                return false;
            } else {
                std::this_thread::yield();
            }
        }
    }

    /**
     * @brief Return an approximate total size across all shards.
     *
     * Uses atomic head and tail snapshots for each shard. If head and
     * tail wrap past capacity the occupancy is clamped at cap per shard.
     * The value is approximate and may race with concurrent operations.
     *
     * @return Estimated total number of elements stored.
     */
    size_t size_approx() const {
        size_t total = 0;

        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            const Shard& s = shards[i];

            size_t t = s.tail.load(std::memory_order_acquire);
            size_t h = s.head.load(std::memory_order_acquire);

            if (t >= h) {
                size_t diff = t - h;
                total += (diff > cap ? cap : diff);
            } else {
                total += cap;
            }
        }

        return total;
    }

private:
    /**
     * @brief Per shard state for the lock free ring buffer.
     *
     * Each shard has its own slot array and atomic head and tail
     * indices. Slots hold a sequence number used to coordinate
     * producer and consumer progress.
     */
    struct Shard {
        Slot* slots = nullptr;
        std::atomic<size_t> head;
        std::atomic<size_t> tail;
    };

    size_t total_cap;
    size_t cap;
    Shard shards[NUM_SHARDS];
};
