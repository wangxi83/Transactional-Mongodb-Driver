package com.sobey.jcg.sobeyhive.sidistran.mongo2.ex;

/**
 * Created by WX on 2016/1/22.
 */
public class LockTimeOutException extends RuntimeException {
    public LockTimeOutException() {
    }

    public LockTimeOutException(String message) {
        super(message);
    }

    public LockTimeOutException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockTimeOutException(Throwable cause) {
        super(cause);
    }

    public LockTimeOutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
