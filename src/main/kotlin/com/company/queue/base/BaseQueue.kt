package com.company.queue.base

/**
 * Created by remote on 9/15/18.
 */
interface BaseQueue<E> {
    fun put(item: E)

    fun load(): List<Node<E>>
}