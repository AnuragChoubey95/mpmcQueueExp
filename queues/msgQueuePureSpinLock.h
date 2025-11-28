#pragma once
#include <cstdint>
#include <cstring>
#include <atomic>
#include <chrono>
#include "slot.h"
#include "../utils/metrics.h"

/**
 * @class MessageQueueMutex
 * @brief Spinlock-protected fixed-size MPMC ring buffer.
 *
 * Same semantics as before, but now ALL synchronization uses
 * the SpinLock. No mutex, no blocking, no OS scheduling path.
 */
class MessageQueueMutex {
public:

    MessageQueueMutex(size_t capacity = CAPACITY)
        : cap(capacity),
          head_index(0),
          tail_index(0),
          full_flag(false),
          count(0)
    {
        slots = new Slot[cap];
    }

    ~MessageQueueMutex() {
        delete[] slots;
    }

    // ------------------------------------------------------------
    // ENQUEUE (spinlock)
    // ------------------------------------------------------------
    bool enqueue(const uint8_t* data, ThreadMetrics& local, size_t worker_id) {
        if (!data) return false;

        uint64_t wait_start = Instrumentation::now_ns();
        lock.lock();                   // spin until acquired
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (full_flag) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            lock.unlock();
            return false;
        }

        std::memcpy(slots[tail_index].data, data, BLOCK_SIZE);

        tail_index = (tail_index + 1) % cap;
        count++;

        if (tail_index == head_index)
            full_flag = true;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        lock.unlock();
        return true;
    }

    // ------------------------------------------------------------
    // DEQUEUE (spinlock)
    // ------------------------------------------------------------
    bool dequeue(uint8_t* out_data, ThreadMetrics& local, size_t worker_id) {
        if (!out_data) return false;

        uint64_t wait_start = Instrumentation::now_ns();
        lock.lock();  
        uint64_t wait_end = Instrumentation::now_ns();
        Instrumentation::on_lock_wait(local, wait_end - wait_start);

        uint64_t crit_start = Instrumentation::now_ns();

        if (!full_flag && head_index == tail_index) {
            uint64_t crit_end = Instrumentation::now_ns();
            Instrumentation::on_critical_section(local, crit_end - crit_start);
            lock.unlock();
            return false;
        }

        std::memcpy(out_data, slots[head_index].data, BLOCK_SIZE);

        head_index = (head_index + 1) % cap;
        count--;
        full_flag = false;

        uint64_t crit_end = Instrumentation::now_ns();
        Instrumentation::on_critical_section(local, crit_end - crit_start);

        lock.unlock();
        return true;
    }

private:
    Slot* slots;
    size_t cap;

    size_t head_index;
    size_t tail_index;
    bool full_flag;
    size_t count;

    SpinLock lock;     // <-- the ONLY lock now
};
