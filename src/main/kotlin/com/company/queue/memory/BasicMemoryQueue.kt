package com.company.queue.memory

import com.company.queue.BasicMessage

class BasicMemoryQueue(batchSize: Int) : MemoryQueue<BasicMessage>(batchSize)