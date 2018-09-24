package com.company.queue.base

import com.company.queue.base.Status.*
import java.time.Instant
import java.time.Instant.now
import java.time.temporal.ChronoUnit.MILLIS

/**
 * Created by remote on 9/15/18.
 */
abstract class CommonNode<E> : Node<E> {

    override fun inTransaction(block: Node<E>.() -> Unit) {
        val started = now()
        outer@ while (true) {
            timeoutChecks(this, started)
            val nextNode = next.get()
            if (nextNode == null || (nextNode.lock.compareAndSet(null, this))) {
                while (true) {
                    if (lock.compareAndSet(null, this)) {
                        try {
                            markDirty()
                            block.invoke(this)
                            clearDirtyState(this, this.formerNextNode.get())
                        } finally {
                            if (startedTransactionTime.get() != null) {
                                unmarkDirty(this, true)
                            }
                        }
                        return
                    } else {
                        val lockNode = lock.get()
                        if (lockNode != null) {
                            if (lockNode.isIn(COMPLETED)) {
                                lock.compareAndSet(lockNode, null)
                            } else if (timeoutChecks(lockNode, started)) {
                                nextNode?.lock?.compareAndSet(this, null)
                                continue@outer
                            }
                        }
                    }
                }
            } else {
                timeoutChecks(nextNode, started)
            }
        }
    }

    fun timeoutChecks(node: Node<E>, started: Instant): Boolean {
        if (started.plus(50 * WAITING_TIMEOUT, MILLIS) < now()) {
            throw RuntimeException("Exceeded waiting time in more then 50 times")
        }
        return if (isInconsistent(node)) {
            unmarkDirty(node)
            true
        } else {
            checkTimeout(started, node)
        }
    }

    private fun checkTimeout(started: Instant, node: Node<E>): Boolean {
        if (started.plus(WAITING_TIMEOUT, MILLIS) < now()) {
            val lock = node.lock.get()
            if (lock != null && node.next.get().let { it == null || it.lock.get() == lock }) {
                if (lock.dirtyTransactionState.compareAndSet(false, true)) {
                    lock.startedTransactionTime.set(now())
                    unmarkDirty(node, true)
                }
                return true
            }
        }
        return false
    }

    override fun processSuccess(force: Boolean) {
        compareAndSet(SENT, CONFIRMED)
        if (isIn(CONFIRMED) && (isNotSentRecently() || force)) {
            sent.set(now())
            if (compareAndSet(CONFIRMED, SENT) ) {
                removeElement(CONFIRMED)
            }
        }
    }

    override fun processFailure(force: Boolean) {
        compareAndSet(SENT, FAILURE)
        if (isIn(FAILURE)  && (isNotSentRecently() || force)) {
            sent.set(now())
            if (compareAndSet(FAILURE, SENT) ) {
                removeElement(FAILURE)
            }
        }
    }

    override fun isFailedByTimeout(): Boolean =
            if (isIn(SENT)) isNotSentRecently() else isIn(FAILURE)

    override fun isNotSentRecently() = sent.get()?.plus(timeout, MILLIS).let { it == null || it < now() }

    override fun isNotResentRecently() = resent.get()?.plus(REMOVING_TIMEOUT, MILLIS).let { it == null || it < now() }

    override fun isIn(vararg statuses: Status) = status.get() in statuses

    override fun compareAndSet(old: Status, new: Status) = status.compareAndSet(old, new)

    private fun removeElement(status: Status) {
        inTransaction {
            processRemovingElement(status)
        }
    }

    fun Node<E>.processRemovingElement(status: Status) {
        this.status.set(status)
        val nextNode = next.get()
        nextNode?.previous?.compareAndSet(this, formerPreviousNode.get())
        if (isIn(FAILURE)) {
            if (isResentRecently()) {
                return
            }
            compareAndSet(FAILURE, RESENDING)
            resent.set(now())
            sent.set(null)
            if (queue!!.head.get() != this) {
                next.set(null)
                queue!!.put(this)
            } else {
                if (!queue!!.head.compareAndSet(this, previous.get()!!)) {
                    next.set(null)
                    queue!!.put(this)
                } else {
                    nextNode?.previous?.compareAndSet(formerPreviousNode.get(), this)
                    compareAndSet(RESENDING, RESENDING_FINISHED_COMPLETED)
                    return
                }
            }
            compareAndSet(RESENDING, RESENDING_FINISHED)
        }
        previous.get().let { it != null && it.next.compareAndSet(this, nextNode) }
        compareAndSet(RESENDING_FINISHED, RESENDING_FINISHED_COMPLETED)
        compareAndSet(CONFIRMED, COMPLETED)
    }

    private fun markDirty() {
        startedTransactionTime.set(java.time.Instant.now())
        dirtyTransactionState.set(true)
        formerNextNode.set(next.get())
        formerPreviousNode.set(previous.get())
    }

    companion object {
        const val REMOVING_TIMEOUT: Long = 2
        const val WAITING_TIMEOUT: Long = 400
        const val PROCESSING_TIMEOUT: Long = 1000

        private fun <E> clearDirtyState(lockNode: Node<E>, formerNextNode: Node<E>?) {
            lockNode.formerNextNode.set(null)
            lockNode.formerPreviousNode.set(null)
            lockNode.lock.compareAndSet(lockNode, null)
            formerNextNode?.lock?.compareAndSet(lockNode, null)
            lockNode.dirtyTransactionState.set(false)
            lockNode.startedTransactionTime.set(null)
        }

        private fun <E> processFailureResentFinished(formerNextNode: Node<E>?, lockNode: Node<E>) {
            formerNextNode?.previous?.compareAndSet(lockNode.previous.get(), lockNode)
            lockNode.compareAndSet(RESENDING_FINISHED, COMPLETED)
        }

        private fun <E> isInconsistent(lockNode: Node<E>?, times: Int = 1) = lockNode != null
                && lockNode.dirtyTransactionState.get()
                && (lockNode.startedTransactionTime.get()?.plus(times * REMOVING_TIMEOUT, MILLIS).let { it == null || it < now() })

        private fun <E> unmarkDirty(lockNode: Node<E>, bypassLock: Boolean = false) {
            val formerNextNode = lockNode.formerNextNode.get()

            if (!bypassLock && !getTemporaryLock(lockNode)) return

            when (lockNode.status.get()) {
                CONFIRMED -> if (lockNode.previous.get().let { it != null &&  it.next.get() != formerNextNode }) {
                    formerNextNode?.previous?.set(lockNode)
                } else {
                    lockNode.compareAndSet(CONFIRMED, COMPLETED)
                }
                FAILURE -> formerNextNode?.previous?.compareAndSet(formerNextNode, lockNode)
                RESENDING -> if (lockNode.temporaryPreviousNode.get() != null
                        && lockNode.temporaryPreviousNode.get() != lockNode.formerPreviousNode.get()) {
                    lockNode.compareAndSet(RESENDING, RESENDING_FINISHED)
                    processFailureResentFinished(formerNextNode, lockNode)
                } else {
                    lockNode.next.compareAndSet(null, lockNode.formerNextNode.get())
                    lockNode.compareAndSet(RESENDING, FAILURE)
                }
                RESENDING_FINISHED -> processFailureResentFinished(formerNextNode, lockNode)
                SENT, NEW, COMPLETED, RESENDING_FINISHED_COMPLETED -> {
                }
            }

            clearDirtyState(lockNode, formerNextNode)
        }

        private fun <E> getTemporaryLock(lockNode: Node<E>): Boolean {
            var lock = false
            if (isInconsistent(lockNode) && !isInconsistent(lockNode, 2)) {
                val secondLock = lockNode.synchronisationStatus.compareAndSet(false, true)
                if (secondLock && isInconsistent(lockNode)) {
                    lockNode.startedTransactionTime.set(now())
                    lock = secondLock && lockNode.synchronisationStatus.compareAndSet(true, false)
                }
            }
            if (isInconsistent(lockNode, 3)) {
                lockNode.startedTransactionTime.set(now())
                lockNode.synchronisationStatus.set(false)
            }
            return lock
        }
    }


}