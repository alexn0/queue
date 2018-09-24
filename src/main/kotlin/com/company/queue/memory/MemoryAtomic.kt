package com.company.queue.memory

import com.company.queue.base.Atomic
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by remote on 9/15/18.
 */
class MemoryAtomic<E>(value: E) : Atomic<E> {
    private val atomicValue: AtomicReference<E> = AtomicReference(value)

    override fun get(): E = atomicValue.get()

    override fun compareAndSet(oldValue: E, newValue: E): Boolean = atomicValue.compareAndSet(oldValue, newValue)

    override fun set(value: E) = atomicValue.set(value)

}
