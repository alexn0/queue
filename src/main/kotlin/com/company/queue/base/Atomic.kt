package com.company.queue.base

/**
 * Created by alexn0 on 9/15/18.
 */
interface Atomic<E> : Strict<E> {
    fun compareAndSet(oldValue: E, newValue: E): Boolean
}
