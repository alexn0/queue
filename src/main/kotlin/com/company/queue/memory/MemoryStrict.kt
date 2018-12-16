package com.company.queue.memory

import com.company.queue.base.Strict

/**
 * Created by alexn0 on 9/15/18.
 */
class MemoryStrict<E>(@Volatile private var value: E) : Strict<E> {
    override fun get(): E = value

    override fun set(value: E) {
        this.value = value
    }
}