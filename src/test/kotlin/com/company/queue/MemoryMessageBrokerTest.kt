package com.company.queue

import com.company.queue.CommonBrokerUtil.Companion.getMemoryBroker
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicBoolean

class MemoryMessageBrokerTest {
    val toMemoryBroker: (batchSize: Int) -> CommonMessageBroker = { CommonBrokerUtil.getMemoryBroker(it) }
    val util = CommonBrokerUtil()

    @Test(timeout = 10000)
    fun `new memory queue is empty`() =
            util.`new queue is empty`(getMemoryBroker())


    @Test(timeout = 10000)
    fun `batch is working for memory queue`() =
            util.`batch is working`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `queue timeout is working for memory queue`() =
            util.`queue timeout is working`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `queue is simply working for memory queue`() =
            util.`queue is simply working`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `commit method is working in the middle for memory queue`() =
            util.`commit method is working in the middle`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `commit method is working in the head for memory queue`() {
        util.`commit method is working in the head`(toMemoryBroker)
    }


    @Test(timeout = 10000)
    fun `commit method is working in the tail for memory queue`() =
            util.`commit method is working in the tail`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `fail method is working in the middle for memory queue`() =
            util.`fail method is working in the middle`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `fail method is working in the head for memory queue`() =
            util.`fail method is working in the head`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `fail method is working in the tail for memory queue`() =
            util.`fail method is working in the tail`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `elements is in right queues for memory queue`() =
            util.`elements is in right queues`(toMemoryBroker)


    @Test(timeout = 10000)
    fun `memory queue is not empty`() {
        val brocker = CommonBrokerUtil.getMemoryBroker()
        val set = HashSet<String>()
        val amount = 100000
        for (i in 0..amount - 1) {
            set.add(brocker.send(CommonBrokerUtil.BASE_QUEUE_NAME, i.toString()))
        }
        Assert.assertEquals(amount, set.size)
        for (i in 0..300) {
            for (message in brocker.receive(CommonBrokerUtil.BASE_QUEUE_NAME)) {
                if (message.id in set) {
                    set.remove(message.id)
                } else throw Exception()
            }
        }
        Assert.assertNotEquals(0, set.size)
    }


    @Test(timeout = 20000)
    fun `check concurrent use of the queue`() {
        val hasException = AtomicBoolean(false)
        val amountOfThread = 9

        val barrier = CyclicBarrier(amountOfThread)
        val latch = CountDownLatch(amountOfThread);
        val broker = CommonBrokerUtil.getMemoryBroker(1000)
        val queueWrapper = QueueWrapper(broker, CommonBrokerUtil.BASE_QUEUE_NAME)
        var i = 0
        while (i < amountOfThread) {
            Thread(QueueWorker(barrier, queueWrapper, latch, hasException)).start()
            i++
        }
        latch.await()
        Assert.assertEquals(false, hasException.get())
    }


    class QueueWorker(val barrier: CyclicBarrier, val queueWrapper: QueueWrapper, val latch: CountDownLatch, val hasException: AtomicBoolean) : Runnable {
        override fun run() {
            barrier.await()

            try {
                queueWrapper
                        .addElements(50000)
                        .receiveElements(5, isToCommit = true)
                        .receiveElements(2, isToCommit = true)
                        .receiveElements(1, isToFail = true)
                        .receiveElements(1)
                while (queueWrapper.broker.receive(CommonBrokerUtil.BASE_QUEUE_NAME).size != 0) {
                    queueWrapper.receiveElements(5, isToCommit = true)
                            .receiveElements(2, isToCommit = true)
                            .receiveElements(1, isToFail = true)

                }
            } catch (e: Exception) {
                hasException.set(true)
                e.printStackTrace()
            } finally {
                latch.countDown()
            }

        }
    }

}