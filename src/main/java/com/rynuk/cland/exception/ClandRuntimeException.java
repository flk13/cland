package com.rynuk.cland.exception;

/**
 * @author rynuk
 * @date 2020/7/28
 */
public abstract class ClandRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ClandRuntimeException() {
    }

    public ClandRuntimeException(String message) {
        super(message);
    }

    public static class FilterOverflowException extends ClandRuntimeException {

        public FilterOverflowException() {
        }

        public FilterOverflowException(String message) {
            super(message);
        }
    }

    public static class OperationFailedException extends ClandRuntimeException {

        public OperationFailedException() {
        }

        public OperationFailedException(String message) {
            super(message);
        }
    }
}
