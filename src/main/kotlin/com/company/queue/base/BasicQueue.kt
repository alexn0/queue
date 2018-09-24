package com.company.queue.base

/**
 * Created by remote on 9/15/18.
 */
interface BasicQueue<E> {
    val head: Atomic<Node<E>>

    fun put(node: Node<E>)

    fun poll(): List<Node<E>>
}