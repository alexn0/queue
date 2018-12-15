package com.company.queue.base

import java.time.Instant
import java.time.temporal.ChronoUnit

object TransactionWrapper {
    @JvmStatic
    val isStarted = ThreadLocal.withInitial({false})
    const val REMOVING_TIMEOUT: Long = 20
    const val TRANSACTION_WAITING_TIMEOUT: Long = 2000
    const val PROCESSING_TIMEOUT: Long = 1000

    @JvmStatic
    fun <E> inTransaction(currentNode: Node<E>, block: Node<E>.() -> Unit) {
       if (isStarted.get()) {
           block.invoke(currentNode)
           return
       }
       try {
           isStarted.set(true)
           executeInTransaction(currentNode, block)
       } finally {
           isStarted.set(false)
       }
    }

    fun <E> executeInTransaction(currentNode: Node<E>, block: Node<E>.() -> Unit) {
        var started = Instant.now()
        outer@ while (true) {
            if (waitingTooLong(started, currentNode.timeout)) {
                started = Instant.now()
                consistentChecks(currentNode)
            }
            val nextNode = currentNode.next.get()
            if (!isInDirtyState(nextNode) && getLockForNextNodeIfPresented(nextNode, currentNode)) {
                while (true) {
                    if (!isInDirtyState(currentNode) && getLockForCurrentNode(currentNode)) {
                        if (nextNode != currentNode.next.get()) {
                            started = Instant.now()
                            releaseLocks(nextNode, currentNode)
                            continue@outer
                        }
                        try {
                            markDirtyState(currentNode)
                            block.invoke(currentNode)
                            clearDirtyState(currentNode, currentNode.formerNextNode.get())
                        } finally {
                            if (isInDirtyState(currentNode)) {
                                unmarkDirtyState(currentNode, true)
                            }
                        }
                        return
                    } else if (nextNode != currentNode.next.get()) {
                        releaseLocks(nextNode, currentNode, false)
                        continue@outer
                    } else if (waitingTooLong(started, currentNode.timeout)) {
                        started = Instant.now()
                        currentNode.lock.get()?.let { consistentChecks(it) }
                        releaseLocks(nextNode, currentNode, false)
                        continue@outer
                    }
                }
            } else if (waitingTooLong(started, currentNode.timeout)) {
                started = Instant.now()
                consistentChecks(nextNode!!)
            }
        }
    }


    private fun <E> getLockForNextNodeIfPresented(nextNode: Node<E>?, node:  Node<E>) =
            nextNode.let { it == null || (it.lock.compareAndSet(null, node)) }


    private fun <E> releaseLocks(node: Node<E>?, current: Node<E>, isCurrentToRelease: Boolean = true) {
        node?.lock?.compareAndSet(current, null)
        if (isCurrentToRelease) {
            current.lock.compareAndSet(current, null)
        }
    }


    private fun <E> consistentChecks(node: Node<E>): Boolean {
        return if (isInconsistent(node)) {
            unmarkDirtyState(node)
            true
        } else false
    }


    private fun <E> markDirtyState(node: Node<E>) {
        node.startedTransactionTime.set(java.time.Instant.now())
        node.dirtyTransactionState.set(true)
        node.formerNextNode.set(node.next.get())
        node.formerPreviousNode.set(node.previous.get())
    }


    private fun <E> isInDirtyState(lockNode: Node<E>?): Boolean {
        return lockNode != null && (lockNode.startedTransactionTime.get() != null)
    }


    private fun <E> clearDirtyState(lockNode: Node<E>, formerNextNode: Node<E>?) {
        lockNode.formerNextNode.set(null)
        lockNode.formerPreviousNode.set(null)
        lockNode.lock.compareAndSet(lockNode, null)
        formerNextNode?.lock?.compareAndSet(lockNode, null)
        lockNode.dirtyTransactionState.set(false)
        lockNode.startedTransactionTime.set(null)
    }


    private fun <E> getLockForCurrentNode(node: Node<E>) = node.lock.compareAndSet(null, node)


    private fun waitingTooLong(started: Instant, timeout: Long) = started.plus(maxOf(TRANSACTION_WAITING_TIMEOUT, 2 * timeout), ChronoUnit.MILLIS) < Instant.now()


    private fun <E> isInconsistent(lockNode: Node<E>?) = lockNode != null
            && lockNode.dirtyTransactionState.get()
            && (lockNode.startedTransactionTime.get()?.plus(minOf(REMOVING_TIMEOUT, lockNode.timeout/10), ChronoUnit.MILLIS).let { it == null || it < Instant.now() })


    private fun <E> unmarkDirtyState(lockNode: Node<E>, bypassLock: Boolean = false) {
        val formerNextNode = lockNode.formerNextNode.get()

        if (!bypassLock && !getTemporaryLock(lockNode)) return

        when (lockNode.status.get()) {
            Status.CONFIRMED -> if (lockNode.previous.get().let { it != null &&  it.next.get() != formerNextNode }) {
                formerNextNode?.previous?.set(lockNode)
            } else {
                lockNode.compareAndSet(Status.CONFIRMED, Status.COMPLETED)
            }
            Status.FAILURE -> formerNextNode?.previous?.compareAndSet(formerNextNode, lockNode)
            Status.RESENDING -> if (lockNode.temporaryPreviousNode.get() != null
                    && lockNode.temporaryPreviousNode.get() != lockNode.formerPreviousNode.get()) {
                lockNode.compareAndSet(Status.RESENDING, Status.RESENDING_FINISHED)
                processFailureResentFinished(formerNextNode, lockNode)
            } else {
                lockNode.next.compareAndSet(null, lockNode.formerNextNode.get())
                lockNode.compareAndSet(Status.RESENDING, Status.FAILURE)
            }
            Status.RESENDING_FINISHED -> processFailureResentFinished(formerNextNode, lockNode)
            Status.SENT, Status.NEW, Status.COMPLETED, Status.RESENDING_FINISHED_COMPLETED -> {
            }
        }

        clearDirtyState(lockNode, formerNextNode)
    }


    private fun <E> getTemporaryLock(lockNode: Node<E>): Boolean {
        var lock = false
        while (true) {
            if (!lockNode.synchronisationStatus.get() && isInconsistent(lockNode)) {
                lockNode.startedTransactionTime.set(Instant.now())
                val secondLock = lockNode.synchronisationStatus.compareAndSet(false, true)
                if (secondLock) {
                    lock = secondLock && lockNode.synchronisationStatus.compareAndSet(true, false)
                }
            } else if (!lockNode.synchronisationStatus.get() && isInconsistent(lockNode)) {
                lockNode.synchronisationStatus.compareAndSet(true, false)
                continue
            }
            break
        }
        return lock
    }


    private fun <E> processFailureResentFinished(formerNextNode: Node<E>?, lockNode: Node<E>) {
        formerNextNode?.previous?.compareAndSet(lockNode.previous.get(), lockNode)
        lockNode.compareAndSet(Status.RESENDING_FINISHED, Status.COMPLETED)
    }


}