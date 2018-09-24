package com.company.queue.disk

import com.company.queue.BasicMessage
import com.company.queue.base.Atomic
import com.company.queue.base.BasicQueue
import com.company.queue.base.Node
import com.company.queue.disk.AbstractDiskNode.Companion.NULL
import java.io.File
import java.nio.file.*

fun createAtomicNode(element: Node<BasicMessage>, baseDir: Path, id: String, name: String = "this", queue: BasicQueue<BasicMessage>?): Atomic<Node<BasicMessage>> {
    val converter = fun(value: Node<BasicMessage>): String = value.item!!.id
    return DiskAtomic(element, getFullPathOfField(baseDir, id, name), { loadNode(baseDir, it, queue)!! }, converter)
}

fun getFullPathOfField(baseDir: Path, id: String, name: String): Path {
    return Paths.get(getDiskNodePath(baseDir, id).toString(), name)
}

fun getDiskNodePath(baseDir: Path, id: String): Path {
    return Paths.get(baseDir.toString(), id.substring(0, 2), id)
}

fun loadNode(baseDir: Path, id: String, queue: BasicQueue<BasicMessage>? = null): Node<BasicMessage>? {
    return if (id == NULL) return null
    else DiskNode(id, baseDir, queue = queue)
}

fun writeTempFile(text: String): Path {
    return doWithAttempts {
        val path = File.createTempFile("queueDisk", ".temp").toPath()
        Files.write(path, text.toByteArray(), StandardOpenOption.CREATE)
    }
}

fun createFile(path: Path) {
    doWithAttempts {
        if (!Files.exists(path)) {
            val from = writeTempFile("")
            doWithAttempts {
                replaceFile(from, path)
            }
        }
    }
}

fun <E> doWithAttempts(operation: () -> E): E {
    var attempt = 0
    while (true) {
        attempt++
        try {
            return operation.invoke()
        } catch (e: Exception) {
            if (attempt > 5) {
                throw e
            }
        }
    }
}

fun readFile(path: Path): String {
    return doWithAttempts {
        String(Files.readAllBytes(path))
    }
}

fun replaceFile(from: Path, to: Path) {
    doWithAttempts {
        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING)
    }
}