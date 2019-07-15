package com.jpro.base;

public class KafkaDataBlock<T, K> {
    private T head;
    private K body;

    public KafkaDataBlock(T a, K b) {
        head = a;
        body = b;
    }

    public T getHead() {
        return head;
    }

    public K getBoby() {
        return body;
    }
}
