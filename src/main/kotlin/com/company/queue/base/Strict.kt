package com.company.queue.base

/**
 * Created by remote on 9/15/18.
 */
interface Strict<E> {
    fun get(): E
    fun set(value: E)
}