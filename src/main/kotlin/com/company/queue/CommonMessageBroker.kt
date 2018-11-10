package com.company.queue

import com.company.queue.base.BaseQueue
import com.company.MessageBroker
import java.util.*
import java.util.concurrent.ConcurrentHashMap

abstract class CommonMessageBroker : MessageBroker<CommonMessage> {
    private val map = ConcurrentHashMap<String, BaseQueue<BasicMessage>>()

    fun getQueue(queueName: String): BaseQueue<BasicMessage> {
        if (!map.containsKey(queueName)) {
            map.putIfAbsent(queueName, createBasicQueue(queueName))
        }
        val queue = map[queueName]!!
        return queue
    }

    abstract protected fun createBasicQueue(queueName: String): BaseQueue<BasicMessage>

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

