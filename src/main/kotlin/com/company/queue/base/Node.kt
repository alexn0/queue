package com.company.queue.base

import java.time.Instant

/**
 * Created by remote on 9/15/18.
 */
interface Node<E> {
    val item: E?
    var queue: BasicQueue<E>?
    val timeout: Long
    val next: Atomic<Node<E>?>
    val previous: Atomic<Node<E>?>
    val lock: Atomic<Node<E>?>
    val status: Atomic<Status>
    val synchronisationStatus: Atomic<Boolean>
    val nextBatch: Strict<Atomic<Node<E>?>>
    val counter: Strict<Int>
    val sent: Strict<Instant?>
    val resent: Strict<Instant?>
    val dirtyTransactionState: Atomic<Boolean>
    val startedTransactionTime: Strict<Instant?>
    val formerNextNode: Strict<Node<E>?>
    val formerPreviousNode: Strict<Node<E>?>
    val temporaryPreviousNode: Strict<Node<E>?>

    fun inTransaction(block: Node<E>.() -> Unit)
    fun compareAndSet(old: Status, new: Status): Boolean
    fun isIn(vararg statuses: Status): Boolean

    fun getAtomicBatchElement(node: Node<E>?): Atomic<Node<E>?>

    fun getNext() = next.get()
    fun processSuccess(force: Boolean = false)
    fun processFailure(force: Boolean = false)
    fun isFailedByTimeout(): Boolean
    fun isNotSentRecently(): Boolean
    fun isNotResentRecently(): Boolean

    fun isSentRecently(): Boolean = !isNotSentRecently()
    fun isResentRecently(): Boolean = !isNotResentRecently()
}