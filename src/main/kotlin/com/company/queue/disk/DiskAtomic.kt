package com.company.queue.disk

import com.company.queue.base.Atomic
import java.nio.file.*

/**
 * Created by alexn0 on 9/15/18.
 */
open class DiskAtomic<E>(value: E,
                         path: Path,
                         toValue: (String) -> E,
                         toString: (E) -> String)
    : DiskStrict<E>(value, path, toValue, toString), Atomic<E> {

    override fun compareAndSet(oldValue: E, newValue: E): Boolean {
        var res = false
        execute(path, true) {
            if (get(false) == oldValue) {
                set(newValue, false)
                res = true
            }
        }
        return res
    }
}
