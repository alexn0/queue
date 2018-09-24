package com.company.queue.memory

import com.company.queue.base.*
import java.nio.file.Path

/**
 * Created by remote on 9/10/18.
 */
open class MemoryQueue<E>(batchSize: Int, private val commonQueue: CommonQueue<E> = MemoryQueue.build(batchSize)) : BaseQueue<E> {

    override fun load(): List<Node<E>> = commonQueue.poll().map { it }.toList()

    override fun put(item: E) = commonQueue.put(item)


    companion object {
        private fun <E> build(batchSize: Int): CommonQueue<E> {
            val dummyNode: Node<E> = MemoryNode(null, null, null)
            return CommonQueue(MemoryAtomic(dummyNode), MemoryAtomic(dummyNode), MemoryAtomic(dummyNode), NodeBuilder(), batchSize = batchSize)
        }

        class NodeBuilder<E> : (E?, Node<E>?, Long, Path?) -> Node<E> {
            override fun invoke(item: E?, next: Node<E>?, timeout: Long, path: Path?): Node<E> =
                    MemoryNode(item, next, null, timeout)
        }
    }

}