package mscalejoin.common;

import com.google.common.collect.ObjectArrays;
import scalegate.ScaleGateTuple;

import java.util.concurrent.atomic.AtomicInteger;

public class Tuple implements Comparable<ScaleGateTuple>, ScaleGateTuple {
    private final long timestamp;
    private final Stream source;
    private final Object[] attributes;
    private final int probeId;
    private final AtomicInteger counter;

    public Tuple(long timestamp, Stream source, Object[] attributes, int probeId) {
        this.source = source;
        this.attributes = attributes;
        this.timestamp = timestamp;
        this.probeId = probeId;
        this.counter = new AtomicInteger();
    }

    public Tuple merge(Tuple o) {
        Tuple t = new Tuple(timestamp, source, ObjectArrays.concat(attributes, o.getAttributes(), Object.class), probeId + 1);
        t.setCounter(counter.get());
        return t;
    }

    private int getCounter() {
        return counter.get();
    }

    public void setCounter(int counter) {
        this.counter.compareAndSet(0, counter);
    }

    public int compareCounterTo(Tuple o) {
        return compareCounterTo(o.getCounter());
    }

    public int compareCounterTo(int counter) {
        if (this.counter.get() == counter) {
            return 0;
        } else {
            return this.counter.get() > counter ? 1 : -1;
        }
    }

    public Stream getSource() {
        return source;
    }

    private Object[] getAttributes() {
        return attributes;
    }

    public Object getAttribute(int id) {
        return attributes[id];
    }

    public int getProbeId() {
        return probeId;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(ScaleGateTuple o) {
        if (this.timestamp == o.getTimestamp()) {
            return 0;
        } else {
            return this.timestamp > o.getTimestamp() ? 1 : -1;
        }
    }
}
