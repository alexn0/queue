package com.company.queue.memory

import com.company.queue.base.*
import java.time.Instant

import com.company.queue.base.Status.*
import com.company.queue.base.TransactionWrapper.PROCESSING_TIMEOUT

class MemoryNode<E>(override val item: E?,
                    next: Node<E>?,
                    override var queue: BasicQueue<E>?,
                    override val timeout: Long = PROCESSING_TIMEOUT) : CommonNode<E>() {

    companion object {
        fun <E> getAtomic(element: E): Atomic<E> = MemoryAtomic(element)

        fun <E> getStrict(element: E): Strict<E> = MemoryStrict(element)
    }

    override val next: Atomic<Node<E>?> = getAtomic(next)
    override val previous: Atomic<Node<E>?> = getAtomic(null)
    override val lock: Atomic<Node<E>?> = getAtomic(null)
    override val status: Atomic<Status> = getAtomic(NEW)
    override val synchronisationStatus: Atomic<Boolean> = getAtomic(false)
    override val nextBatch: Strict<Atomic<Node<E>?>> = getStrict(getAtomic(next))
    override val counter: Strict<Int> = getStrict(-1)
    override val sent: Strict<Instant?> = getStrict(null)
    override val created: Strict<Instant> = getStrict(Instant.now())
    override val resent: Strict<Instant?> = getStrict(null)
    override val dirtyTransactionState: Atomic<Boolean> = getAtomic(false)
    override val startedTransactionTime: Strict<Instant?> = getStrict(null)
    override val formerNextNode: Strict<Node<E>?> = getStrict(null)
    override val formerPreviousNode: Strict<Node<E>?> = getStrict(null)
    override val temporaryPreviousNode: Strict<Node<E>?> = getStrict(null)

    override fun getAtomicBatchElement(node: Node<E>?): Atomic<Node<E>?> = getAtomic(node)

}