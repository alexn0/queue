package com.company


interface Message {
    val id: String

    val body: String

    fun commit()

    fun fail()
}