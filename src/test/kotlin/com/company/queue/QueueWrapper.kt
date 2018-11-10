package com.company.queue

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger


class QueueWrapper(val broker: CommonMessageBroker,
                   private val queueName: String = CommonBrokerUtil.BASE_QUEUE_NAME) {

    private val insertedElements = ConcurrentHashMap<String, BasicMessage>()
    private val mapToMessageId = ConcurrentHashMap<Int, String>()
    private val allReceivedElements = ConcurrentHashMap<String, CommonMessage>()
    private val allReceivedCommitedElements = ConcurrentHashMap<String, BasicMessage>()
    private val allReceivedFailedElements = ConcurrentHashMap<String, BasicMessage>()
    private val indexToMessageId = ConcurrentHashMap<String, Int>()
    private var lastReceivedElementsList: ArrayList<ConcurrentHashMap<String, BasicMessage>> = ArrayList()
    private var lastReceivedElements: ConcurrentHashMap<String, BasicMessage> = ConcurrentHashMap()
    private var elementIndex = AtomicInteger(-1)

    fun isContainedInLastReceivedElements(elem: Int): Boolean {
        return mapToMessageId.get(elem).let { it !=null && lastReceivedElements.containsKey(it) }
    }


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
            val elementIndex = this.elementIndex.incrementAndGet()
            val content = elementIndex.toString()
            val messageId = broker.send(queueName, content)
            mapToMessageId.put(elementIndex, messageId)
            indexToMessageId.put(messageId, elementIndex)
            insertedElements.put(messageId, BasicMessage(messageId, content))
            amountElements++
        }
        return this
    }


    fun receiveElements(times: Int, isToFail: Boolean = false, isToCommit: Boolean = false): QueueWrapper {
        var attempts = 0
        val receivedElementsList = ArrayList<ConcurrentHashMap<String, BasicMessage>>()
        val lastReceivedElements = ConcurrentHashMap<String, BasicMessage>()
        while (attempts < times) {
            val messages = broker.receive(queueName)
            val elements = ConcurrentHashMap<String, BasicMessage>()
            for (elem in messages) {
                val index = elem.id
                val message = BasicMessage(elem.id, elem.body)
                allReceivedElements.put(index, elem)
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
        allReceivedElements[mapToMessageId[index]]!!.fail()
        this
    } else {
        throw IllegalArgumentException()
    }


    fun commit(index: Int): QueueWrapper = if (isReceived(index)) {
        allReceivedElements[mapToMessageId[index]!!]!!.commit()
        this
    } else {
        throw IllegalArgumentException()
    }


    fun isReceived(index: Int) = mapToMessageId[index].let{ it !=null && allReceivedElements.containsKey(it) }


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


