package com.company.queue.base

/**
 * Created by alexn0 on 9/15/18.
 */
interface BaseQueue<E> {
    fun put(item: E)

    fun load(): List<Node<E>>
}