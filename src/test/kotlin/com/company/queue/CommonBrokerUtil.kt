package com.company.queue

import com.company.queue.base.TransactionWrapper.PROCESSING_TIMEOUT
import org.junit.Assert.assertEquals
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class CommonBrokerUtil {

    fun `new queue is empty`(broker: CommonMessageBroker) = assertEquals(0, broker.receive(BASE_QUEUE_NAME).size)


    fun `batch is working`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 10
        val broker = toBrocker.invoke(batchSize)
        val queueWrapper = QueueWrapper(broker)
                .addElements(2 * batchSize)
                .receiveElements(2)
        assertEquals(true, queueWrapper.isAllElementReceived())
    }


    fun `queue timeout is working`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 10
        val broker = toBrocker.invoke(batchSize)
        val queueWrapper = QueueWrapper(broker)
                .addElements(2 * batchSize)
                .receiveElements(1, isToCommit = true)
                .receiveElements(2)
        assertEquals(true, queueWrapper.isAllElementReceived())
        assertEquals(batchSize, queueWrapper.getSizeOfLastReceivedElements(0))
        assertEquals(0, queueWrapper.getSizeOfLastReceivedElements(1))
        waitForRollbackByTimeout()
        queueWrapper.receiveElements(2)
        assertEquals(1, queueWrapper.getSizeOfLastReceivedElements(0))
        assertEquals(batchSize - 1, queueWrapper.getSizeOfLastReceivedElements(1))
    }


    fun `queue is simply working`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 3
        val broker = toBrocker.invoke(batchSize)
        val receiveAttempts = 2
        val amount = batchSize * receiveAttempts
        val queueWrapper = QueueWrapper(broker)
                .addElements(receiveAttempts * batchSize)
                .receiveElements(receiveAttempts)
        assertEquals(true, queueWrapper.isAllElementReceived())
        var i = 0
        while (i < receiveAttempts) {
            assertEquals(batchSize, queueWrapper.getSizeOfLastReceivedElements(i))
            i++
        }
        waitForRollbackByTimeout()
        queueWrapper.receiveElements(receiveAttempts)
        assertEquals(amount, queueWrapper.getSizeOfLastReceivedElements())
    }


    fun `commit method is working in the middle`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        var batchSize = 15
        var middleElements = arrayOf(6, 7, 8)
        checkThatCommitMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)

        batchSize = 1
        middleElements = arrayOf(0)
        checkThatCommitMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)
    }


    fun `commit method is working in the head`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 15
        val middleElements = arrayOf(12, 13, 14)
        checkThatCommitMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)
    }


    fun `commit method is working in the tail`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 15
        val middleElements = arrayOf(0, 1, 2)
        checkThatCommitMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)
    }


    fun `fail method is working in the middle`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        var batchSize = 15
        var middleElements = arrayOf(6, 7, 8)
        checkThatFailMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)

        batchSize = 1
        middleElements = arrayOf(0)
        checkThatFailMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)
    }


    fun `fail method is working in the head`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 15
        val middleElements = arrayOf(12, 13, 14)
        checkThatFailMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)
    }


    fun `fail method is working in the tail`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 15
        val middleElements = arrayOf(0, 1, 2)
        checkThatFailMethodIsCorrect(toBrocker.invoke(batchSize), batchSize, middleElements)
    }


    fun checkThatCommitMethodIsCorrect(broker: CommonMessageBroker, batchSize: Int, elements: Array<Int>) {
        val queueWrapper = QueueWrapper(broker)
                .addElements(batchSize)
                .receiveElements(1)
        assertEquals(true, queueWrapper.isAllElementReceived())
        for (elem in elements) {
            queueWrapper.commit(elem)
        }
        waitForRollbackByTimeout()
        queueWrapper.receiveElements(2)
        assertEquals(batchSize - elements.size, queueWrapper.getSizeOfLastReceivedElements())
        for (elem in elements) {
            assertEquals(false, queueWrapper.isContainedInLastReceivedElements(elem))
        }
    }


    fun checkThatFailMethodIsCorrect(broker: CommonMessageBroker, batchSize: Int, elements: Array<Int>) {
        val queueWrapper = QueueWrapper(broker)
                .addElements(batchSize)
                .receiveElements(1)
        assertEquals(true, queueWrapper.isAllElementReceived())
        for (elem in elements) {
            queueWrapper.fail(elem)
        }
        queueWrapper.receiveElements(2)
        assertEquals(elements.size, queueWrapper.getSizeOfLastReceivedElements())
        for (elem in elements) {
            assertEquals(true, queueWrapper.isContainedInLastReceivedElements(elem))
            queueWrapper.commit(elem)
        }
        waitForRollbackByTimeout()
        queueWrapper.receiveElements(batchSize)
        assertEquals(batchSize - elements.size, queueWrapper.getSizeOfLastReceivedElements())
        for (elem in elements) {
            assertEquals("Contains element: " + elem.toString(), false, queueWrapper.isContainedInLastReceivedElements(elem))
        }
    }


    fun `elements is in right queues`(toBrocker: (batchSize: Int) -> CommonMessageBroker) {
        val batchSize = 10
        val broker = toBrocker.invoke(batchSize)
        val firstQueueWrapper = QueueWrapper(broker, BASE_QUEUE_NAME)
        val secondQueueWrapper = QueueWrapper(broker, SECOND_QUEUE_NAME)
        val thirdQueueWrapper = QueueWrapper(broker, THIRD_QUEUE_NAME)
        firstQueueWrapper.addElements(200)
        assertEquals(0, secondQueueWrapper.receiveElements(1).getSizeOfLastReceivedElements())
        assertEquals(batchSize, firstQueueWrapper.receiveElements(1).getSizeOfLastReceivedElements())
        assertEquals(0, thirdQueueWrapper.receiveElements(1).getSizeOfLastReceivedElements())
    }


    companion object {
        const val BASE_QUEUE_NAME = "first_queue"
        const val SECOND_QUEUE_NAME = "second_queue"
        const val THIRD_QUEUE_NAME = "third_queue"

        fun getTempDir() = Files.createTempDirectory("queue")

        fun waitForRollbackByTimeout() {
            TimeUnit.MILLISECONDS.sleep(PROCESSING_TIMEOUT)
        }

        fun getMemoryBroker(batchSize: Int = 30) = MemoryMessageBroker(batchSize)


        fun getDiskBroker(batchSize: Int = 30, path: Path = CommonBrokerUtil.getTempDir()) = DiskMessageBroker(path, batchSize)

    }
}
