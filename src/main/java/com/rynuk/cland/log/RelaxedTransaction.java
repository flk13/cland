package com.rynuk.cland.log;

import com.rynuk.cland.exception.ClandRuntimeException;

import org.slf4j.Logger;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public class RelaxedTransaction implements Transaction {
    public static void doProcess(Do doOp, Undo undoOp) {
        try {
            doOp.roll();
        } catch (ClandRuntimeException.OperationFailedException e) {
            undoOp.rollback();
        }
    }

    public static void doProcess(Do doOp, Undo undoOp, Logger logger) {
        try {
            doOp.roll();
        } catch (ClandRuntimeException.OperationFailedException e) {
            logger.error(e.getMessage());
            undoOp.rollback();
        }
    }
}
