package com.sobey.jcg.sobeyhive.sidistran.enums;

/**
 * Created by WX on 2015/11/30.
 */
public enum Protocol {
    CLASS,
    SPRING_BEAN,
    DUBBO,
    RMI,
    ZOOKEEPER;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
