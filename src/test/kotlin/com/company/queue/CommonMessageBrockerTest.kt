package com.company.queue;


import com.company.queue.base.CommonNode.Companion.PROCESSING_TIMEOUT
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test;
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class CommonMessageBrockerTest {

    @Test
    fun `new queue is empty`() {
        val brockers = arrayOf(CommonMessageBroker(), CommonMessageBroker(false, getTempDir()))
        for (brocker in brockers) {
            assertEquals(0, brocker.receive(BASE_QUEUE_NAME).size)
        }
    }


    @Test
    fun `batch is working`() {
        val batchSize = 10
        val brockers = arrayOf(getMemoryBroker(batchSize), getDiskBroker(batchSize))
        for (brocker in brockers) {
            val queueWrapper = QueueWrapper(brocker)
                    .addElements(2 * batchSize)
                    .receiveElements(2)
            assertEquals(true, queueWrapper.isAllElementReceived())
        }
    }

    @Test
    fun `queue timeout is working`() {
        val batchSize = 10
        val brockers = arrayOf(getMemoryBroker(batchSize), getDiskBroker(batchSize))
        for (brocker in brockers) {
            val queueWrapper = QueueWrapper(brocker)
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
    }

    @Test
    fun `queue is simply working`() {
        val batchSize = 3
        val receiveAttempts = 2
        val amount = batchSize * receiveAttempts
        val brockers = arrayOf(getMemoryBroker(batchSize), getDiskBroker(batchSize))
        for (brocker in brockers) {
            val queueWrapper = QueueWrapper(brocker)
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
    }


    @Test
    fun `memory queue is not empty`() {
        val brocker = CommonMessageBroker()
        val set = HashSet<String>()
        val amount = 100000
        for (i in 0..amount - 1) {
            set.add(brocker.send(BASE_QUEUE_NAME, i.toString()))
        }
        assertEquals(amount, set.size)
        for (i in 0..300) {
            for (message in brocker.receive(BASE_QUEUE_NAME)) {
                if (message.id in set) {
                    set.remove(message.id)
                } else throw Exception()
            }
        }
        assertNotEquals(0, set.size)
    }


    @Test
    fun `disk queue is simply working`() {
        val brocker = CommonMessageBroker(false, getTempDir(), 300)
        val set = HashSet<String>()
        val amount = 1000
        for (i in 0..amount - 1) {
            set.add(brocker.send(BASE_QUEUE_NAME, i.toString()))
        }
        assertEquals(amount, set.size)
        for (i in 0..4) {
            val messages = brocker.receive(BASE_QUEUE_NAME)
            for (message in messages) {
                if (message.id in set) {
                    message.commit()
                    set.remove(message.id)
                } else throw Exception()
            }
        }
        assertEquals(0, set.size)
        waitForRollbackByTimeout()
        val messages = brocker.receive(BASE_QUEUE_NAME)
        assertEquals(0, messages.size)
    }

    @Test
    fun `commit method is working in the middle`() {
        var batchSize = 15
        var middleElements = arrayOf(6, 7, 8)
        checkThatCommitMethodIsCorrect(batchSize, middleElements)

        batchSize = 1
        middleElements = arrayOf(0)
        checkThatCommitMethodIsCorrect(batchSize, middleElements)
    }

    @Test
    fun `commit method is working in the head`() {
        val batchSize = 15
        val middleElements = arrayOf(12, 13, 14)
        checkThatCommitMethodIsCorrect(batchSize, middleElements)
    }

    @Test
    fun `commit method is working in the tail`() {
        val batchSize = 15
        val middleElements = arrayOf(0, 1, 2)
        checkThatCommitMethodIsCorrect(batchSize, middleElements)
    }

    @Test
    fun `fail method is working in the middle`() {
        var batchSize = 15
        var middleElements = arrayOf(6, 7, 8)
        checkThatFailMethodIsCorrect(batchSize, middleElements)

        batchSize = 1
        middleElements = arrayOf(0)
        checkThatFailMethodIsCorrect(batchSize, middleElements)
    }

    @Test
    fun `fail method is working in the head`() {
        val batchSize = 15
        val middleElements = arrayOf(12, 13, 14)
        checkThatFailMethodIsCorrect(batchSize, middleElements)
    }

    @Test
    fun `fail method is working in the tail`() {
        val batchSize = 15
        val middleElements = arrayOf(0, 1, 2)
        checkThatFailMethodIsCorrect(batchSize, middleElements)
    }

    fun checkThatCommitMethodIsCorrect(batchSize: Int, elements: Array<Int>) {
        val brockers = arrayOf(getMemoryBroker(batchSize), getDiskBroker(batchSize))
        for (brocker in brockers) {
            val queueWrapper = QueueWrapper(brocker)
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
                assertEquals(false, queueWrapper.lastReceivedElements.containsKey(elem))
            }
        }
    }

    fun checkThatFailMethodIsCorrect(batchSize: Int, elements: Array<Int>) {
        val brockers = arrayOf(getMemoryBroker(batchSize), getDiskBroker(batchSize))
        for (brocker in brockers) {
            val queueWrapper = QueueWrapper(brocker)
                    .addElements(batchSize)
                    .receiveElements(1)
            assertEquals(true, queueWrapper.isAllElementReceived())
            for (elem in elements) {
                queueWrapper.fail(elem)
            }
            queueWrapper.receiveElements(2)
            assertEquals(elements.size, queueWrapper.getSizeOfLastReceivedElements())
            for (elem in elements) {
                assertEquals(true, queueWrapper.lastReceivedElements.containsKey(elem))
                queueWrapper.commit(elem)
            }
            waitForRollbackByTimeout()
            queueWrapper.receiveElements(2)
            assertEquals(batchSize - elements.size, queueWrapper.getSizeOfLastReceivedElements())
            for (elem in elements) {
                assertEquals("Contains element: " + elem.toString(), false, queueWrapper.lastReceivedElements.containsKey(elem))
            }
        }
    }

    @Test
    fun `elements is in right queues`() {
        val batchSize = 10
        val brockers = arrayOf(getMemoryBroker(batchSize), getDiskBroker(batchSize))
        for (brocker in brockers) {
            val firstQueueWrapper = QueueWrapper(brocker, BASE_QUEUE_NAME)
            val secondQueueWrapper = QueueWrapper(brocker, SECOND_QUEUE_NAME)
            val thirdQueueWrapper = QueueWrapper(brocker, THIRD_QUEUE_NAME)
            firstQueueWrapper.addElements(200)
            assertEquals(0, secondQueueWrapper.receiveElements(1).getSizeOfLastReceivedElements())
            assertEquals(batchSize, firstQueueWrapper.receiveElements(1).getSizeOfLastReceivedElements())
            assertEquals(0, thirdQueueWrapper.receiveElements(1).getSizeOfLastReceivedElements())
        }
    }

    @Test
    fun `disk based queue is working after restart`() {
        val path = getTempDir()
        val firstBroker = CommonMessageBroker(false, path, 300)
        val set = HashSet<String>()
        val amount = 1000
        for (i in 0..amount - 1) {
            set.add(firstBroker.send(BASE_QUEUE_NAME, i.toString()))
        }
        assertEquals(amount, set.size)
        for (i in 0..2) {
            val messages = firstBroker.receive(BASE_QUEUE_NAME)
            for (message in messages) {
                if (message.id in set) {
                    message.commit()
                    set.remove(message.id)
                } else throw Exception()
            }
        }
        assertNotEquals(0, set.size)
        val secondBroker = CommonMessageBroker(false, path, 300)
        val lastMessages = secondBroker.receive(BASE_QUEUE_NAME)
        for (message in lastMessages) {
            if (message.id in set) {
                message.commit()
                set.remove(message.id)
            } else throw Exception()
        }
        assertEquals(0, set.size)
        waitForRollbackByTimeout()
        val messages = firstBroker.receive(BASE_QUEUE_NAME)
        assertEquals(0, messages.size)
    }

    @Test
    fun `check concurrent use of the queue`() {
        val hasException = AtomicBoolean(false)
        val amountOfThread = 3
        val barrier = CyclicBarrier(amountOfThread)
        val latch = CountDownLatch(amountOfThread);
        val broker = getMemoryBroker(1000)
        var i = 0
        while (i < amountOfThread) {
            Thread(QueueWorker(barrier, broker, latch, hasException)).start()
            i++
        }
        latch.await()
        assertEquals(false, hasException.get())
    }


    class QueueWorker(val barrier: CyclicBarrier, val broker: CommonMessageBroker, val latch: CountDownLatch, val hasException: AtomicBoolean) : Runnable {
        override fun run() {
            barrier.await()
            val queueWrapper = QueueWrapper(broker, BASE_QUEUE_NAME)
            try {
                queueWrapper
                        .addElements(50000)
                        .receiveElements(5, isToCommit = true, isToAddStatistics = false)
                        .receiveElements(2, isToCommit = true, isToAddStatistics = false)
                        .receiveElements(1, isToFail = true, isToAddStatistics = false)
                        .receiveElements(1, isToAddStatistics = false)
                while (broker.receive(BASE_QUEUE_NAME).size != 0) {
                    queueWrapper.receiveElements(5, isToCommit = true, isToAddStatistics = false)
                            .receiveElements(2, isToCommit = true, isToAddStatistics = false)
                            .receiveElements(1, isToFail = true, isToAddStatistics = false)

                }
            } catch (e: Exception) {
                hasException.set(true)
                e.printStackTrace()
            } finally {
                latch.countDown()
            }

        }
    }


    companion object {
        const val BASE_QUEUE_NAME = "first_queue"
        const val SECOND_QUEUE_NAME = "second_queue"
        const val THIRD_QUEUE_NAME = "third_queue"

        private fun getTempDir() = Files.createTempDirectory("queue")

        fun waitForRollbackByTimeout() {
            TimeUnit.MILLISECONDS.sleep(PROCESSING_TIMEOUT)
        }

        class QueueWrapper(private val queueBrocker: CommonMessageBroker, private val queueName: String = BASE_QUEUE_NAME) {
            val insertedElements = HashMap<Int, BasicMessage>()
            val allReceivedElements = HashMap<Int, CommonMessage>()
            val allReceivedCommitedElements = HashMap<Int, BasicMessage>()
            val allReceivedFailedElements = HashMap<Int, BasicMessage>()
            private val indexToMessageId = HashMap<String, Int>()
            lateinit var lastReceivedElementsList: ArrayList<HashMap<Int, BasicMessage>>
            lateinit var lastReceivedElements: HashMap<Int, BasicMessage>
            private var elementIndex = -1

            fun getSizeOfLastReceivedElements(): Int {
                return lastReceivedElements.size
            }

            fun getSizeOfLastReceivedElements(index: Int): Int {
                if (lastReceivedElementsList.size > index && index >= 0) {
                    return lastReceivedElementsList[index].size
                }
                throw IllegalArgumentException()
            }

            fun addElements(amount: Int): QueueWrapper {
                var amountElements = 0
                while (amountElements < amount) {
                    elementIndex++
                    val content = elementIndex.toString()
                    val messageId = queueBrocker.send(queueName, content)
                    insertedElements.put(elementIndex, BasicMessage(messageId, content))
                    indexToMessageId.put(messageId, elementIndex)
                    amountElements++
                }
                return this
            }

            fun receiveElements(times: Int, isToFail: Boolean = false, isToCommit: Boolean = false, isToAddStatistics: Boolean = true): QueueWrapper {
                var attempts = 0
                val receivedElementsList = ArrayList<HashMap<Int, BasicMessage>>()
                val lastReceivedElements = HashMap<Int, BasicMessage>()
                while (attempts < times) {
                    val messages = queueBrocker.receive(queueName)
                    val elements = HashMap<Int, BasicMessage>()
                    for (elem in messages) {
                        val index = if (isToAddStatistics) indexToMessageId[elem.id] else -1
                        val message = BasicMessage(elem.id, elem.body)
                        allReceivedElements.put(index!!, elem)
                        elements.put(index, message)
                        lastReceivedElements.put(index, message)
                        if (isToFail) {
                            elem.fail()
                            allReceivedFailedElements.put(index, message)
                        }
                        if (isToCommit) {
                            elem.commit()
                            allReceivedCommitedElements.put(index, message)
                        }
                    }
                    receivedElementsList.add(elements)
                    attempts++
                }
                this.lastReceivedElementsList = receivedElementsList
                this.lastReceivedElements = lastReceivedElements
                return this
            }

            fun fail(index: Int): QueueWrapper = if (isReceived(index)) {
                allReceivedElements[index]!!.fail()
                this
            } else {
                throw IllegalArgumentException()
            }

            fun commit(index: Int): QueueWrapper = if (isReceived(index)) {
                allReceivedElements[index]!!.commit()
                this
            } else {
                throw IllegalArgumentException()
            }

            fun isReceived(index: Int) = allReceivedElements.containsKey(index)

            fun doOperation(start: Int, end: Int, operation: (Int) -> Boolean): QueueWrapper {
                var begin = start
                if (begin >= end || end < 0) throw IllegalArgumentException()
                if (begin < 0) begin = 0
                while (begin < end) {
                    if (!operation(begin)) {
                        throw IllegalArgumentException()
                    }
                    begin++
                }
                return this
            }

            fun isAllElementReceived(): Boolean {
                if (insertedElements.size == allReceivedElements.size) {
                    for (key in insertedElements.keys) {
                        if (allReceivedElements.containsKey(key)) {
                            val elem = allReceivedElements.get(key)
                            if (insertedElements.get(key) != BasicMessage(elem!!.id, elem.body))
                                return false
                        } else {
                            return false
                        }
                    }
                }
                return true
            }

        }

        fun getMemoryBroker(batchSize: Int): CommonMessageBroker {
            return CommonMessageBroker(true, batchSize = batchSize)
        }

        fun getDiskBroker(batchSize: Int, path: Path = getTempDir()): CommonMessageBroker {
            return CommonMessageBroker(false, path, batchSize)
        }

    }
}
