package com.sobey.jcg.sobeyhive.sidistran.mongo2.ex;

/**
 * Created by WX on 2016/1/13.
 */
public class SidistranMongoCuccrentException extends RuntimeException{
    public SidistranMongoCuccrentException() {
    }

    public SidistranMongoCuccrentException(String message) {
        super(message);
    }

    public SidistranMongoCuccrentException(String message, Throwable cause) {
        super(message, cause);
    }

    public SidistranMongoCuccrentException(Throwable cause) {
        super(cause);
    }

    public SidistranMongoCuccrentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
