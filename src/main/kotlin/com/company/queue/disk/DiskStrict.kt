package com.company.queue.disk

import com.company.queue.base.Strict
import java.io.RandomAccessFile
import java.nio.channels.FileLock
import java.nio.file.*
import java.time.Instant.now
import java.time.temporal.ChronoUnit

/**
 * Created by alexn0 on 9/15/18.
 */
open class DiskStrict<E>(value: E,
                         val path: Path,
                         private val toValue: (String) -> E,
                         private val toString: (E) -> String) : Strict<E> {

    init {
        if (!Files.exists(path)) {
            path.parent.toFile().mkdirs()
            set(value, true)
        } else {
            get(true)
        }
    }

    override fun get(): E = get(true)

    override fun set(value: E) = set(value, true)

    protected fun get(isInLock: Boolean): E = execute(path, isInLock) {
        load()
    }

    protected fun set(value: E, isInLock: Boolean) = execute(path, isInLock) {
        update(value)
    }

    private fun load(): E = toValue(readFile(path))

    private fun update(value: E) {
        val from = writeTempFile(toString(value))
        replaceFile(from, path)
    }

    companion object {
        private const val LOCK_WAITING_TIME: Long = 3000

        fun <E> execute(path: Path, isInLock: Boolean, operation: () -> E): E {
            if (!isInLock) {
                return operation.invoke()
            }
            val lockPath = Paths.get(path.toString() + ".lock")
            var res: E
            val started = now()

            while (true) {
                if (started.plus(LOCK_WAITING_TIME, ChronoUnit.MILLIS) < now()) {
                    throw RuntimeException("Exceeded waiting time in getting file lock")
                }
                if (Files.exists(lockPath) && !Files.isDirectory(lockPath)) {
                    val randomAccessFile = RandomAccessFile(lockPath.toFile(), "rw")
                    val fc = randomAccessFile.channel
                    randomAccessFile.use {
                        fc.use {
                            var fileLock: FileLock? = null
                            try {
                                fileLock = fc.tryLock()
                                if (fileLock != null) {
                                    res = operation()
                                    fileLock.release()
                                    fileLock = null
                                    return res
                                }
                            } finally {
                                fileLock?.release()
                            }
                        }
                    }
                } else if (!Files.exists(lockPath)) {
                    createFile(lockPath)
                } else {
                    throw IllegalArgumentException()
                }
            }
        }
    }
}