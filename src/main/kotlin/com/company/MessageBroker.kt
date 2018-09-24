package com.company

interface MessageBroker<out T : Message> {

    fun send(queue: String, message: String): String

    fun receive(queue: String): List<T>
}