package com.rynuk.cland.log;

import com.rynuk.cland.exception.ClandRuntimeException;

/**
 * @author rynuk
 * @date 2020/7/25
 */
@FunctionalInterface
public interface Undo {
    void rollback() throws ClandRuntimeException.OperationFailedException;
}
