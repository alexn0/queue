package com.company.queue.base

import com.company.queue.base.Status.*
import java.time.Instant.now
import java.time.temporal.ChronoUnit.MILLIS

/**
 * Created by remote on 9/15/18.
 */
abstract class CommonNode<E> : Node<E> {

    override fun inTransaction(block: Node<E>.() -> Unit) {
        TransactionWrapper.inTransaction(this, block)
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

}