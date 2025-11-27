#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>
#include <thread>
#include "slot.h"
#include "../utils/metrics.h"

// Source: https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

/**
 * @brief Single shard lock free atomic ring buffer queue.
 *
 * Implements a sequence based ring buffer using atomic head and tail
 * indices. Producers and consumers operate without locks using the
 * standard single producer multiple producer algorithm: each slot has
 * a sequence number that determines whether it is ready for enqueue,
 * dequeue, or free. Threads retry on CAS failure and yield under
 * contention. Operations are non blocking and return false when the
 * queue appears full or empty.
 */
class MessageQueueAtomic {
public:
    /**
     * @brief Construct a lock free queue with a fixed capacity.
     *
     * Each slot is initialized with a sequence number matching its
     * logical index. A capacity of zero is coerced to one.
     *
     * @param capacity Number of elements in the ring buffer.
     */
    MessageQueueAtomic(size_t capacity = CAPACITY)
        : cap(capacity),
          head(0),
          tail(0)
    {
        if (cap == 0)
            cap = 1;

        slots = new Slot[cap];

        for (size_t i = 0; i < cap; ++i) {
            slots[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    /**
     * @brief Destroy the queue and release slot storage.
     */
    ~MessageQueueAtomic() {
        delete[] slots;
    }

    /**
     * @brief Attempt to enqueue data into the queue.
     *
     * The caller repeatedly loads tail, checks the sequence number of
     * the slot, and attempts a CAS to claim the index. On successful
     * reservation the data is written and the sequence number is
     * advanced. CAS attempts and failures are logged in the provided
     * thread metrics. Returns false when the queue is full.
     *
     * @param data Pointer to BLOCK_SIZE bytes to enqueue.
     * @param local Thread local metrics.
     * @param worker_id Unused parameter.
     * @return true on success, false if queue is full or data is null.
     */
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        while (true) {
            size_t pos = tail.load(std::memory_order_relaxed);
            Slot& slot = slots[pos % cap];

            size_t seq = slot.seq.load(std::memory_order_acquire);
            intptr_t diff =
                static_cast<intptr_t>(seq) -
                static_cast<intptr_t>(pos);

            if (diff == 0) {
                Instrumentation::on_cas_attempt(local);

                if (!tail.compare_exchange_weak(
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
            }
            else if (diff < 0) {
                return false;
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    /**
     * @brief Attempt to dequeue a message from the queue.
     *
     * Follows the same sequence based strategy as enqueue. The caller
     * repeatedly loads head, checks the slot sequence number, attempts
     * a CAS to claim the slot, and on success copies the data out and
     * frees the slot by advancing its sequence to a future cycle.
     * Returns false when the queue appears empty.
     *
     * @param out_data Output buffer for BLOCK_SIZE bytes.
     * @param local Thread local metrics.
     * @param worker_id Unused parameter.
     * @return true on success, false if queue is empty or buffer null.
     */
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        while (true) {
            size_t pos = head.load(std::memory_order_relaxed);
            Slot& slot = slots[pos % cap];

            size_t seq = slot.seq.load(std::memory_order_acquire);
            intptr_t diff =
                static_cast<intptr_t>(seq) -
                static_cast<intptr_t>(pos + 1);

            if (diff == 0) {
                Instrumentation::on_cas_attempt(local);

                if (!head.compare_exchange_weak(
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
            }
            else if (diff < 0) {
                return false;
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    /**
     * @brief Return an approximate occupancy of the queue.
     *
     * Computes a difference between atomic tail and head snapshots.
     * On wrap ambiguity the queue reports full capacity.
     *
     * @return Estimated number of elements stored.
     */
    size_t size_approx() const {
        size_t t = tail.load(std::memory_order_acquire);
        size_t h = head.load(std::memory_order_acquire);

        if (t >= h) {
            size_t diff = t - h;
            return diff > cap ? cap : diff;
        }

        return cap;
    }

private:
    Slot* slots;
    size_t cap;

    std::atomic<size_t> head;
    std::atomic<size_t> tail;
};
