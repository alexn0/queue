package com.company.queue

import com.company.queue.base.BaseQueue
import com.company.queue.disk.BasicDiskQueue
import java.nio.file.Path
import java.nio.file.Paths

class DiskMessageBroker(private val path: Path, private val batchSize: Int = 30) : CommonMessageBroker() {

    override fun createBasicQueue(queueName: String): BaseQueue<BasicMessage> {
        return BasicDiskQueue(Paths.get(path.toString(), queueName), batchSize)
    }

}
