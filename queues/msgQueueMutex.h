#pragma once
#include <cstdint>
#include <cstring>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include "slot.h"
#include "../utils/metrics.h"

/**
 * @class MessageQueueMutex
 * @brief Fixed-size MPMC circular queue protected by a single global mutex.
 *
 * This queue provides a simple bounded ring buffer shared by multiple
 * producers and consumers. All operations serialize through one mutex,
 * making correctness trivial but limiting scalability under contention.
 *
 * Instrumentation hooks record:
 *  - Lock wait time
 *  - Critical-section duration
 *  - Enqueue/dequeue failures (full/empty)
 *
 * The queue is non-blocking: enqueue() returns false when full,
 * dequeue() returns false when empty.
 */
class MessageQueueMutex {
public:

    /**
     * @brief Construct a bounded ring buffer backed by a single mutex.
     *
     * @param capacity Number of message slots in the queue.
     *        Defaults to CAPACITY. Must be >= 1.
     */
    MessageQueueMutex(size_t capacity = CAPACITY)
        : cap(capacity),
          head_index(0),
          tail_index(0),
          full_flag(false),
          count(0)
    {
        slots = new Slot[cap];
    }

    /**
     * @brief Destroy the queue and release internal storage.
     *
     * The destructor assumes the queue is no longer accessed by any
     * producers or consumers. No synchronization is performed here.
     */
    ~MessageQueueMutex() {
        delete[] slots;
    }

    /**
     * @brief Enqueue one message into the queue.
     *
     * Operation is serialized via a global mutex. If the queue is full,
     * the function immediately returns false without modifying state.
     *
     * Instrumentation recorded:
     *  - Time spent waiting for the mutex
     *  - Time spent inside the critical section
     *
     * @param data Pointer to BLOCK_SIZE bytes containing the message.
     * @param local Thread-local metrics structure updated by Instrumentation.
     * @param worker_id Logical worker identifier (unused in this queue).
     * @return true if the message is enqueued, false if full or data is null.
     */
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        uint64_t wait_start = Instrumentation::now_ns();
        mtx.lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (full_flag) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            mtx.unlock();
            return false;
        }

        std::memcpy(slots[tail_index].data, data, BLOCK_SIZE);

        tail_index = (tail_index + 1) % cap;
        count++;

        if (tail_index == head_index)
            full_flag = true;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        mtx.unlock();
        return true;
    }

    /**
     * @brief Dequeue one message from the queue.
     *
     * Operation is serialized through a global mutex. If the queue
     * is empty, the function returns false immediately.
     *
     * Instrumentation recorded:
     *  - Time spent waiting for the mutex
     *  - Time spent inside the critical section
     *
     * @param out_data Caller-provided buffer of BLOCK_SIZE bytes.
     * @param local Thread-local metrics structure updated by Instrumentation.
     * @param worker_id Logical worker identifier (unused in this queue).
     * @return true on success, false if the queue is empty or buffer is null.
     */
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        uint64_t wait_start = Instrumentation::now_ns();
        mtx.lock();
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (!full_flag && head_index == tail_index) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            mtx.unlock();
            return false;
        }

        std::memcpy(out_data, slots[head_index].data, BLOCK_SIZE);

        head_index = (head_index + 1) % cap;
        count--;
        full_flag = false;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        mtx.unlock();
        return true;
    }

private:
    Slot* slots;        
    size_t cap;         

    size_t head_index;  
    size_t tail_index;  
    bool full_flag;     
    size_t count;       

    std::mutex mtx;     
};
