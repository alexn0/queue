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
        var started = now()
        outer@ while (true) {
            if (waitingTooLong(started)) {
                started = now()
                consistentChecks(this)
            }
            val nextNode = next.get()
            if (!isInDirtyState(nextNode) && getLockForNextNodeIfPresented(nextNode)) {
                while (true) {
                    if (!isInDirtyState(this) && getLockForCurrentNode()) {
                        if (nextNode != next.get()) {
                            started = now()
                            releaseLocks(nextNode, this)
                            continue@outer
                        }
                        try {
                            markDirtyState()
                            block.invoke(this)
                            clearDirtyState(this, this.formerNextNode.get())
                        } finally {
                            if (isInDirtyState(this)) {
                                unmarkDirtyState(this, true)
                            }
                        }
                        return
                    } else if (nextNode != next.get()) {
                        releaseLocks(nextNode, this, false)
                        continue@outer
                    } else if (waitingTooLong(started)) {
                        started = now()
                        lock.get()?.let { consistentChecks(it) }
                        releaseLocks(nextNode, this, false)
                        continue@outer
                    }
                }
            } else if (waitingTooLong(started)) {
                started = now()
                consistentChecks(nextNode!!)
            }
        }
    }

    fun releaseLocks(node: Node<E>?, current: Node<E>, isCurrentToRelease: Boolean = true) {
        node?.lock?.compareAndSet(current, null)
        if (isCurrentToRelease) {
            current.lock.compareAndSet(current, null)
        }
    }

    fun getLockForCurrentNode() = lock.compareAndSet(null, this)

    fun getLockForNextNodeIfPresented(nextNode: Node<E>?) =
            nextNode.let { it == null || (it.lock.compareAndSet(null, this)) }

    fun waitingTooLong(started: Instant) = started.plus(TRANSACTION_WAITING_TIMEOUT, MILLIS) < now()

    fun consistentChecks(node: Node<E>): Boolean {
        return if (isInconsistent(node)) {
            unmarkDirtyState(node)
            true
        } else false
    }

    override fun processSuccess(force: Boolean) {
        inTransaction {
            compareAndSet(SENT, CONFIRMED)
            if (isIn(CONFIRMED) && (isNotSentRecently() || force)) {
                sent.set(now())
                processRemovingElement()
            }
        }
    }

    override fun processFailure(force: Boolean) {
        inTransaction {
            compareAndSet(SENT, FAILURE)
            if (isIn(FAILURE) && (isNotSentRecently() || force)) {
                sent.set(now())
                processRemovingElement()
            }
        }
    }

    override fun isFailedByTimeout(): Boolean =
            if (isIn(SENT)) isNotSentRecently() else isIn(FAILURE)

    override fun isNotSentRecently() = sent.get()?.plus(timeout, MILLIS).let { it == null || it < now() }

    override fun isNotResentRecently() = resent.get()?.plus(timeout, MILLIS).let { it == null || it < now() }

    override fun isNotCreatedRecently() = created.get().plus(timeout, MILLIS) < now()

    override fun isIn(vararg statuses: Status) = status.get() in statuses

    override fun compareAndSet(old: Status, new: Status) = status.compareAndSet(old, new)

    fun Node<E>.processRemovingElement() {
        this.counter.set(-1)
        val nextNode = next.get()
        nextNode?.previous?.compareAndSet(this, formerPreviousNode.get())
        if (isIn(FAILURE)) {
            if (isResentRecently()) {
                return
            }
            compareAndSet(FAILURE, RESENDING)
            resent.set(now())
            sent.set(null)
            val q = queue!!
            if (q.head.get() != this) {
                next.set(null)
                q.put(this)
            } else {
                if (!q.head.compareAndSet(this, previous.get()!!)) {
                    next.set(null)
                    q.put(this)
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

    private fun markDirtyState() {
        startedTransactionTime.set(java.time.Instant.now())
        dirtyTransactionState.set(true)
        formerNextNode.set(next.get())
        formerPreviousNode.set(previous.get())
    }

    companion object {
        const val REMOVING_TIMEOUT: Long = 20
        const val TRANSACTION_WAITING_TIMEOUT: Long = 2000
        const val PROCESSING_TIMEOUT: Long = 1000

        private fun <E> clearDirtyState(lockNode: Node<E>, formerNextNode: Node<E>?) {
            lockNode.formerNextNode.set(null)
            lockNode.formerPreviousNode.set(null)
            lockNode.lock.compareAndSet(lockNode, null)
            formerNextNode?.lock?.compareAndSet(lockNode, null)
            lockNode.dirtyTransactionState.set(false)
            lockNode.startedTransactionTime.set(null)
        }

        private fun <E> isInDirtyState(lockNode: Node<E>?): Boolean {
            return lockNode != null && (lockNode.startedTransactionTime.get() != null)
        }

        private fun <E> processFailureResentFinished(formerNextNode: Node<E>?, lockNode: Node<E>) {
            formerNextNode?.previous?.compareAndSet(lockNode.previous.get(), lockNode)
            lockNode.compareAndSet(RESENDING_FINISHED, COMPLETED)
        }

        private fun <E> isInconsistent(lockNode: Node<E>?) = lockNode != null
                && lockNode.dirtyTransactionState.get()
                && (lockNode.startedTransactionTime.get()?.plus(REMOVING_TIMEOUT, MILLIS).let { it == null || it < now() })

        private fun <E> unmarkDirtyState(lockNode: Node<E>, bypassLock: Boolean = false) {
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
            while (true) {
                if (lockNode.synchronisationStatus.get() == false && isInconsistent(lockNode)) {
                    lockNode.startedTransactionTime.set(now())
                    val secondLock = lockNode.synchronisationStatus.compareAndSet(false, true)
                    if (secondLock) {
                        lock = secondLock && lockNode.synchronisationStatus.compareAndSet(true, false)
                    }
                } else if (lockNode.synchronisationStatus.get() == false && isInconsistent(lockNode)) {
                    lockNode.synchronisationStatus.compareAndSet(true, false)
                    continue
                }
                break
            }
            return lock
        }
    }


}