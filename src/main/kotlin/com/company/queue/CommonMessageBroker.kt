package com.company.queue

import com.company.queue.base.BaseQueue
import com.company.queue.disk.BasicDiskQueue
import com.company.queue.memory.BasicMemoryQueue
import com.company.MessageBroker
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

class CommonMessageBroker(private val isMemory: Boolean = true, private val path: Path? = null, val batchSize: Int = 30) : MessageBroker<CommonMessage> {
    init {
        if (!isMemory && path == null) {
            throw IllegalArgumentException("Path should be defined for disk based queue")
        }
    }

    private val map = HashMap<String, BaseQueue<BasicMessage>>()

    private fun createBasicQueue(queueName: String): BaseQueue<BasicMessage> {
        return if (isMemory) BasicMemoryQueue(batchSize) else BasicDiskQueue(Paths.get(path.toString(), queueName), batchSize)
    }

    private fun getQueue(queueName: String): BaseQueue<BasicMessage> {
        if (!map.containsKey(queueName)) {
            map.putIfAbsent(queueName, createBasicQueue(queueName))
        }
        val queue = map[queueName]!!
        return queue
    }

    override fun send(queue: String, message: String): String {
        val baseQueue = getQueue(queue)
        val id = UUID.randomUUID().toString()
        baseQueue.put(BasicMessage(id, message))
        return id
    }

    override fun receive(queue: String): List<CommonMessage> {
        val baseQueue = getQueue(queue)
        return baseQueue.load().map { CommonMessage(it) }.toList()
    }

}

