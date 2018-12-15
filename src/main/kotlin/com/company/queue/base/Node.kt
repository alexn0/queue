package com.company.queue.base

import java.time.Instant

/**
 * Created by remote on 9/15/18.
 */
interface Node<E> {
    //content
    val item: E?
    //queue parameters
    var queue: BasicQueue<E>?
    val timeout: Long
    //node next and previous links
    val next: Atomic<Node<E>?>
    val previous: Atomic<Node<E>?>
    //for batch
    val nextBatch: Strict<Atomic<Node<E>?>>
    val counter: Strict<Int>
    //delivery and transaction status parameters
    val lock: Atomic<Node<E>?>
    val status: Atomic<Status>
    val synchronisationStatus: Atomic<Boolean>
    val created: Strict<Instant>
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

    fun isNotCreatedRecently(): Boolean

    fun isSentRecently(): Boolean = !isNotSentRecently()

    fun isResentRecently(): Boolean = !isNotResentRecently()

    fun isCreatedRecently(): Boolean = !isNotCreatedRecently()
}