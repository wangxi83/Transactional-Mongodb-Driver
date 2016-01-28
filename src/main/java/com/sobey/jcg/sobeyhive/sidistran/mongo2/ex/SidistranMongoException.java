package com.sobey.jcg.sobeyhive.sidistran.mongo2.ex;

/**
 * Created by WX on 2015/12/29.
 */
public class SidistranMongoException extends RuntimeException {
    public SidistranMongoException() {
    }

    public SidistranMongoException(String message) {
        super(message);
    }

    public SidistranMongoException(String message, Throwable cause) {
        super(message, cause);
    }

    public SidistranMongoException(Throwable cause) {
        super(cause);
    }

    public SidistranMongoException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
