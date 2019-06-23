package com.github.dfauth.dbstreamer;

public interface Sink<R> {
    void push(R r);
}
