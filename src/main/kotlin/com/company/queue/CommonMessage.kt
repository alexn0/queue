package com.company.queue

import com.company.Message
import com.company.queue.base.Node

/**
 * Created by remote on 9/10/18.
 */
class CommonMessage(private val node: Node<BasicMessage>,
                    override val id: String = node.item!!.id,
                    override val body: String = node.item!!.body) : Message {

    override fun commit() {
        node.processSuccess(true)
    }

    override fun fail() {
        node.processFailure(true)
    }
}