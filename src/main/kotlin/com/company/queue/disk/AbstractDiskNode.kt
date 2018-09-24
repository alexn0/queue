package com.company.queue.disk

import com.company.queue.BasicMessage
import com.company.queue.base.*
import java.time.Instant

import com.company.queue.base.Status.*
import com.company.queue.memory.MemoryAtomic
import java.nio.file.Path

abstract class AbstractDiskNode(private val id: String,
                                val baseDir: Path,
                                private val localNext: Node<BasicMessage>?,
                                override var queue: BasicQueue<BasicMessage>?,
                                item: BasicMessage?,
                                override val timeout: Long = PROCESSING_TIMEOUT) : CommonNode<BasicMessage>() {

    companion object {
        const val NULL = "null"
        const val BATCH = "batch"
        const val LINK_TO_BATCH = "linkToBatch"
        const val ID = "id"

        private val toNodeId = fun(value: Node<BasicMessage>?): String = value?.item?.id ?: NULL

        class Id<E> : (E) -> E {
            override fun invoke(item: E) = item
        }
    }

    protected val bodyWrapped: Strict<String> by lazy {
        DiskStrict(item?.body ?: id, getFullPathOfField("body", id), Id(), Id())
    }

    protected val idWrapped: Strict<String> by lazy {
        DiskStrict(item?.id ?: id, getFullPathOfField(ID, id), Id(), Id())
    }

    override val item: BasicMessage? = BasicMessage(idWrapped.get(), bodyWrapped.get())
    override val next: Atomic<Node<BasicMessage>?>  by lazy { getAtomic(localNext, "next") }
    override val previous: Atomic<Node<BasicMessage>?> by lazy { getAtomic(null, "previous") }
    override val lock: Atomic<Node<BasicMessage>?> by lazy { getAtomic(null, "lock") }
    override val status: Atomic<Status> by lazy { getAtomic(NEW, "status") }
    override val synchronisationStatus: Atomic<Boolean> by lazy { getAtomic(false, "synchronisationStatus") }
    override val nextBatch: Strict<Atomic<Node<BasicMessage>?>> by lazy { getStrictAtomicBatchElement() }
    override val counter: Strict<Int>  by lazy { getAtomic(-1, "counter") }
    override val sent: Strict<Instant?>  by lazy { getAtomicInstant(null, "sent") }
    override val resent: Strict<Instant?>  by lazy { getAtomicInstant(null, "resent") }
    override val dirtyTransactionState: Atomic<Boolean>  by lazy { getAtomic(false, "dirtyTransactionState") }
    override val startedTransactionTime: Strict<Instant?>  by lazy { getAtomicInstant(null, "startedTransactionTime") }
    override val formerNextNode: Strict<Node<BasicMessage>?>  by lazy { getAtomic(null, "formerNextNode") }
    override val formerPreviousNode: Strict<Node<BasicMessage>?>  by lazy { getAtomic(null, "formerPreviousNode") }
    override val temporaryPreviousNode: Strict<Node<BasicMessage>?>  by lazy { getAtomic(null, "temporaryPreviousNode") }

    private fun getFullPathOfField(name: String, nodeId: String): Path = getFullPathOfField(baseDir, nodeId, name)

    private val loadNode = fun(id: String): Node<BasicMessage>? = loadNode(baseDir, id)

    private fun getStrictAtomicBatchElement(): Strict<Atomic<Node<BasicMessage>?>> {
        val toString = fun(value: Atomic<Node<BasicMessage>?>): String = if (value is DiskAtomic) {
            value.path.getParent().getFileName().toString()
        } else NULL

        val toObject = fun(value: String): Atomic<Node<BasicMessage>?> = if (value != NULL) {
            getAtomicBatchElement(loadNode(baseDir, value))
        } else {
            getAtomic(null, BATCH)
        }

        val nodeBatchId = getAtomicFieldValue(id, BATCH, id).get()
        val endNode = loadNode(baseDir, nodeBatchId)
        val element = getAtomicBatchElement(endNode)
        return DiskStrict(element, getFullPathOfField(LINK_TO_BATCH, id), toObject, toString)
    }

    override fun getAtomicBatchElement(node: Node<BasicMessage>?): Atomic<Node<BasicMessage>?> =
            if (node is AbstractDiskNode) {
                DiskAtomic(node, getFullPathOfField(node.baseDir, toNodeId(node), BATCH), node.loadNode, toNodeId)
            } else MemoryAtomic(null)

    private fun getAtomicFieldValue(element: String, name: String, nodeId: String): Atomic<String> =
            DiskAtomic(element, getFullPathOfField(name, nodeId), Id(), Id())

    private fun getAtomic(element: Int, name: String, nodeId: String = id): Atomic<Int> =
            DiskAtomic(element, getFullPathOfField(name, nodeId), String::toInt, Int::toString)

    private fun getAtomic(element: Boolean, name: String, nodeId: String = id): Atomic<Boolean> =
            DiskAtomic(element, getFullPathOfField(name, nodeId), String::toBoolean, Boolean::toString)

    private fun getAtomic(element: Status, name: String, nodeId: String = id): Atomic<Status> =
            DiskAtomic(element, getFullPathOfField(name, nodeId), { Status.valueOf(it) }, { it.name })

    private fun getAtomic(element: Node<BasicMessage>?, name: String, nodeId: String = id): Atomic<Node<BasicMessage>?> =
            DiskAtomic(element, getFullPathOfField(name, nodeId), loadNode, toNodeId)

    private fun getAtomicInstant(element: Instant?, name: String, nodeId: String = id): Atomic<Instant?> {
        val toString = fun(value: Instant?): String = value?.toString() ?: NULL

        val converter = fun(value: String): Instant? =
                if (value != NULL) Instant.parse(value) else null

        return DiskAtomic(element, getFullPathOfField(name, nodeId), converter, toString)
    }

}