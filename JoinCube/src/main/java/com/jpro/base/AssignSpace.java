package com.jpro.base;

import java.util.concurrent.ArrayBlockingQueue;

public class AssignSpace<T> {

    final int queueSize = 50;

    private ArrayBlockingQueue<T> queue;

    AssignSpace(int size) {
        queue = new ArrayBlockingQueue<>(queueSize);
    }

    public T take() throws InterruptedException {
        return queue.take();
    }

    public void put(T x) throws InterruptedException {
        queue.put(x);
    }
}
