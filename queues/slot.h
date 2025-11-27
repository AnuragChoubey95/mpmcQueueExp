#pragma once
#include <atomic>

#define BLOCK_SIZE 64
#define CAPACITY 2048

/**
 * @brief Fixed-size slot used by all queue implementations.
 *
 * Each slot contains a sequence counter and a fixed BLOCK_SIZE payload.
 * In lock-free queues, the sequence number encodes the slot’s current
 * state relative to producer and consumer positions. In locked queues,
 * the sequence counter may be unused but is retained for structural
 * consistency across queue variants.
 */
struct Slot {
    /** @brief Sequence number used for lock-free arbitration. */
    std::atomic<size_t> seq;

    /** @brief Message payload region of fixed BLOCK_SIZE bytes. */
    uint8_t data[BLOCK_SIZE];
};
