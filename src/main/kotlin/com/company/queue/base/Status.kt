package com.company.queue.base

enum class Status {
    NEW,
    SENT,
    CONFIRMED,
    FAILURE,
    RESENDING,
    RESENDING_FINISHED,
    RESENDING_FINISHED_COMPLETED,
    COMPLETED
}