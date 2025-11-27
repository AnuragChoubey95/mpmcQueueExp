#pragma once
#include <cstdint>
#include <cstring>
#include <mutex>
#include "slot.h"
#include "../utils/metrics.h"

/**
 * @brief Two-lock circular buffer queue.
 *
 * Uses separate head and tail mutexes to reduce contention between
 * producers and consumers. Tracks lock-wait and critical-section
 * durations through Instrumentation. Enqueue/dequeue return false
 * when the queue is full or empty respectively.
 */
class MessageQueueTwoLock {
public:
    /**
     * @brief Construct a two-lock queue with a fixed capacity.
     * @param capacity Number of slots in the ring buffer.
     */
    MessageQueueTwoLock(size_t capacity = CAPACITY)
        : cap(capacity),
          head_index(0),
          tail_index(0),
          full_flag(false)
    {
        slots = new Slot[cap];
    }

    /**
     * @brief Destroy the queue and release slot storage.
     */
    ~MessageQueueTwoLock() {
        delete[] slots;
    }

    /**
     * @brief Attempt to enqueue a fixed-size message.
     *
     * Acquires the tail lock, checks for available space, copies the
     * message into the ring, updates tail index, and records metrics.
     *
     * @param data Pointer to BLOCK_SIZE bytes.
     * @param local Thread-local metrics.
     * @param worker_id Worker index (unused).
     * @return true on success, false if queue is full or data is null.
     */
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        uint64_t wait_start = Instrumentation::now_ns();
        tail_lock.lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (full_flag) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            tail_lock.unlock();
            return false;
        }

        std::memcpy(slots[tail_index].data, data, BLOCK_SIZE);
        tail_index = (tail_index + 1) % cap;

        if (tail_index == head_index)
            full_flag = true;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        tail_lock.unlock();
        return true;
    }

    /**
     * @brief Attempt to dequeue a message.
     *
     * Acquires the head lock, checks for available items, copies data
     * into the output buffer, advances the head index, and clears the
     * full flag.
     *
     * @param out_data Caller-provided output buffer.
     * @param local Thread-local metrics.
     * @param worker_id Worker index (unused).
     * @return true on success, false if queue is empty or buffer null.
     */
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        uint64_t wait_start = Instrumentation::now_ns();
        head_lock.lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (!full_flag && head_index == tail_index) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            head_lock.unlock();
            return false;
        }

        std::memcpy(out_data, slots[head_index].data, BLOCK_SIZE);
        head_index = (head_index + 1) % cap;
        full_flag = false;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        head_lock.unlock();
        return true;
    }

private:
    Slot* slots;
    size_t cap;

    size_t head_index;
    size_t tail_index;
    bool full_flag;

    std::mutex head_lock;
    std::mutex tail_lock;
};
