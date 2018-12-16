package com.company.queue.disk

import com.company.queue.BasicMessage
import com.company.queue.base.*
import com.company.queue.base.TransactionWrapper.PROCESSING_TIMEOUT
import java.nio.file.Path

open class DiskNode(id: String,
                    baseDir: Path,
                    item: BasicMessage? = null,
                    next: Node<BasicMessage>? = null,
                    override var queue: BasicQueue<BasicMessage>?,
                    override val timeout: Long = PROCESSING_TIMEOUT) : AbstractDiskNode(id, baseDir, next, queue, item) {
    private val diskNodePath = getDiskNodePath(baseDir, id)

    init {
        diskNodePath.toFile().mkdirs()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DiskNode

        if (idWrapped.get() != other.idWrapped.get()) return false

        return true
    }

    override fun hashCode(): Int {
        return idWrapped.get().hashCode()
    }
}