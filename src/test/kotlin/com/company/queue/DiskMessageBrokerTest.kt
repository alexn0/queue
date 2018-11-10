package com.company.queue

import com.company.queue.CommonBrokerUtil.Companion.getDiskBroker
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test

@Ignore
class DiskMessageBrokerTest {
    val toDiskBroker: (batchSize: Int) -> CommonMessageBroker = { getDiskBroker(it) }
    val util = CommonBrokerUtil()

    @Test(timeout = 10000)
    fun `new disk queue is empty`() =
            util.`new queue is empty`(getDiskBroker())


    @Test(timeout = 10000)
    fun `batch is working for disk queue`() =
            util.`batch is working`(toDiskBroker)


    @Test(timeout = 10000)
    fun `queue timeout is working for disk queue`() =
            util.`queue timeout is working`(toDiskBroker)


    @Test(timeout = 10000)
    fun `queue is simply working for disk queue`() =
            util.`queue is simply working`(toDiskBroker)


    @Test(timeout = 10000)
    fun `commit method is working in the middle for disk queue`() =
            util.`commit method is working in the middle`(toDiskBroker)


    @Test(timeout = 10000)
    fun `commit method is working in the head for disk queue`() =
            util.`commit method is working in the head`(toDiskBroker)


    @Test(timeout = 10000)
    fun `commit method is working in the tail for disk queue`() =
            util.`commit method is working in the tail`(toDiskBroker)


    @Test(timeout = 10000)
    fun `fail method is working in the middle for disk queue`() =
            util.`fail method is working in the middle`(toDiskBroker)


    @Test(timeout = 10000)
    fun `fail method is working in the head for disk queue`() =
            util.`fail method is working in the head`(toDiskBroker)


    @Test(timeout = 10000)
    fun `fail method is working in the tail for disk queue`() =
            util.`fail method is working in the tail`(toDiskBroker)


    @Test(timeout = 10000)
    fun `elements is in right queues for disk queue`() =
            util.`elements is in right queues`(toDiskBroker)


    @Test(timeout = 10000)
    fun `disk queue is simply working`() {
        val brocker = CommonBrokerUtil.getDiskBroker(300, CommonBrokerUtil.getTempDir())
        val set = HashSet<String>()
        val amount = 1000
        for (i in 0..amount - 1) {
            set.add(brocker.send(CommonBrokerUtil.BASE_QUEUE_NAME, i.toString()))
        }
        Assert.assertEquals(amount, set.size)
        for (i in 0..4) {
            val messages = brocker.receive(CommonBrokerUtil.BASE_QUEUE_NAME)
            for (message in messages) {
                if (message.id in set) {
                    message.commit()
                    set.remove(message.id)
                } else throw Exception()
            }
        }
        Assert.assertEquals(0, set.size)
        CommonBrokerUtil.waitForRollbackByTimeout()
        val messages = brocker.receive(CommonBrokerUtil.BASE_QUEUE_NAME)
        Assert.assertEquals(0, messages.size)
    }


    @Test(timeout = 10000)
    fun `disk based queue is working after restart`() {
        val path = CommonBrokerUtil.getTempDir()
        val firstBroker = DiskMessageBroker(path, 300)
        val set = HashSet<String>()
        val amount = 1000
        for (i in 0..amount - 1) {
            set.add(firstBroker.send(CommonBrokerUtil.BASE_QUEUE_NAME, i.toString()))
        }
        Assert.assertEquals(amount, set.size)
        for (i in 0..2) {
            val messages = firstBroker.receive(CommonBrokerUtil.BASE_QUEUE_NAME)
            for (message in messages) {
                if (message.id in set) {
                    message.commit()
                    set.remove(message.id)
                } else throw Exception()
            }
        }
        Assert.assertNotEquals(0, set.size)
        val secondBroker = DiskMessageBroker(path, 300)
        val lastMessages = secondBroker.receive(CommonBrokerUtil.BASE_QUEUE_NAME)
        for (message in lastMessages) {
            if (message.id in set) {
                message.commit()
                set.remove(message.id)
            } else throw Exception()
        }
        Assert.assertEquals(0, set.size)
        CommonBrokerUtil.waitForRollbackByTimeout()
        val messages = firstBroker.receive(CommonBrokerUtil.BASE_QUEUE_NAME)
        Assert.assertEquals(0, messages.size)
    }

}