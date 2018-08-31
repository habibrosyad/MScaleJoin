package mscalejoin;

import mscalejoin.common.Tuple;

import java.util.concurrent.atomic.AtomicReference;

public class BufferNode {
    private final AtomicReference<BufferNode> next;
    private Tuple tuple;

    BufferNode(Tuple tuple) {
        this.tuple = tuple;
        this.next = new AtomicReference<>();
    }

    public Tuple getTuple() {
        return tuple;
    }

    BufferNode getNext() {
        return next.get();
    }

    boolean trySetNext(BufferNode newNode) {
        return next.compareAndSet(null, newNode);
    }
}
