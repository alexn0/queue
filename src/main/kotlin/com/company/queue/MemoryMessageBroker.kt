package com.company.queue

import com.company.queue.base.BaseQueue
import com.company.queue.memory.BasicMemoryQueue

class MemoryMessageBroker(private val batchSize: Int = 30) : CommonMessageBroker() {

    override fun createBasicQueue(queueName: String): BaseQueue<BasicMessage> {
        return BasicMemoryQueue(batchSize)
    }

}
