package com.company.queue.base

import com.company.queue.base.Status.*
import java.nio.file.Path
import java.time.Instant.now
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

/**
 * Created by remote on 9/10/18.
 */
open class CommonQueue<E>(
        override val head: Atomic<Node<E>>,
        private val tail: Atomic<Node<E>>,
        private val processedElementsHead: Atomic<Node<E>>,
        private val createNode: (E?, Node<E>?, Long, Path?) -> Node<E>,
        private val timeout: Long = 1000,
        private val batchSize: Int = 300,
        private val dir: Path? = null) : BasicQueue<E> {

    val resendingStatuses = arrayOf(RESENDING, RESENDING_FINISHED, RESENDING_FINISHED_COMPLETED)

    fun put(item: E) {
        val newNode = createNode(item, null, timeout, dir)
        put(newNode)
    }

    override fun put(node: Node<E>) {
        var success: Boolean
        do {
            val curTail = getLast()

            updateBatchCounters(node, curTail, batchSize)
            if (node.isIn(RESENDING)) {
                node.temporaryPreviousNode.set(curTail.next.get())
            }

            success = curTail.next.compareAndSet(null, node)
            if (success) {
                updateBatchLink(node, curTail)
            }

            tail.compareAndSet(curTail, curTail.getNext()!!)
        } while (!success)
    }

    override fun poll(): List<Node<E>> {
        checkSentNodes()
        while (true) {
            val zero = getZero()
            if (zero == getZero()) {
                val last = getLast()
                val next = getFirstIfExistsOrNull(zero)
                if (zero == last) {
                    if (next == null) {
                        return emptyList()
                    }
                    tail.compareAndSet(last, next)
                } else {
                    next!!.sent.set(now())
                    val nearestBatchElement = getNearestBatchElement(next)
                    if (head.compareAndSet(zero, nearestBatchElement)) {
                        return getProcessedElements(zero, nearestBatchElement)
                    }

                }
            }
        }
    }

    private fun checkSentNodes() {
        val prev = getLastProcessedElement()
        var node = prev.getNext()
        var attempts = 0
        var isContinue = true
        while (node != null && node != getFirstIfExistsOrNull() && attempts < batchSize && isContinue) {
            if (node.isSentRecently() || node.isResentRecently()) {
                return
            }
            if (node == getZero()) {
                isContinue = false
            }
            val status = node.status.get()

            if (status == NEW) {
                node.inTransaction {
                    if (this.compareAndSet(NEW, FAILURE)) {
                        updateProcessedElement(this, now().minus(2 * timeout, ChronoUnit.MILLIS), prev)
                        this.processFailure()
                    }
                }
                continue
            } else if (status in resendingStatuses) {
                if (node.isResentRecently() || !node.status.compareAndSet(status, FAILURE)) {
                    continue
                }
            }

            if (node.isFailedByTimeout()) {
                node.queue = this
                node.processFailure()
            } else if (status == CONFIRMED) {
                node.processSuccess()
            } else {
                return
            }
            node = getLastProcessedElement().getNext()
            attempts++
        }
    }

    private fun getProcessedElements(zero: Node<E>, nearestBatchElement: Node<E>): ArrayList<Node<E>> {
        val result = ArrayList<Node<E>>()
        var node = zero
        val sent = now()
        do {
            val previous = node
            node.getNext().let { if (it != null) node = it else return result}
            updateProcessedElement(node, sent, previous)
            val status = node.status.get()
            if (status in resendingStatuses) {
                node.status.compareAndSet(status, SENT)
            } else {
                node.status.compareAndSet(NEW, SENT)
            }
            if (node.status.get() == SENT) {
                result.add(node)
            }
        } while (node != nearestBatchElement)
        return result
    }

    fun updateProcessedElement(node: Node<E>, sent: java.time.Instant, previous: Node<E>): Node<E> {
        node.sent.set(sent)
        node.resent.set(null)
        node.previous.compareAndSet(null, previous)
        node.queue = this
        if (node.getNext() != null) {
            node.getNext()!!.previous.set(node)
        }
        return node
    }

    fun updateBatchCounters(newNode: Node<E>, curTail: Node<E>, batchSize: Int) {
        with(newNode) {
            counter.set(curTail.counter.get() + 1)
            if (counter.get() >= batchSize) {
                nextBatch.set(getAtomicBatchElement(this))
                counter.set(0)
            } else if (counter.get() > 0) {
                nextBatch.set(curTail.nextBatch.get())
            } else {
                nextBatch.set(getAtomicBatchElement(this))
            }
        }
    }

    fun updateBatchLink(newNode: Node<E>, curTail: Node<E>?) {
        with(newNode) {
            nextBatch.get().compareAndSet(curTail, this)
        }
    }


    private fun getZero() = head.get()
    private fun getLast() = tail.get()
    private fun getFirstIfExistsOrNull(zero: Node<E> = getZero()) = zero.getNext()
    private fun getLastProcessedElement() = processedElementsHead.get()
    private fun getNearestBatchElement(next: Node<E>): Node<E> {
        val nextBatch = next.nextBatch.get().get()!!
        return if (next.counter.get() > nextBatch.counter.get() || next.counter.get() == -1) next else nextBatch
    }

}