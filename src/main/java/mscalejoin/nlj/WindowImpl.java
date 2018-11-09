package mscalejoin.nlj;

import mscalejoin.common.Joiner;
import mscalejoin.common.Probe;
import mscalejoin.common.Tuple;
import mscalejoin.common.Window;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class WindowImpl implements Window {
    private final List<Tuple> internal;
    private final long size;

    public WindowImpl(long size) {
        this.size = size;
        internal = new LinkedList<>();
    }

    @Override
    public void probe(Tuple tuple, Probe probe, Joiner joiner) {
        Predicate predicate = ((ProbeImpl) probe).getPredicate();

        for (Tuple member : internal) {
            if (predicate.compare(tuple, member)) {
                joiner.join(member);
            }
        }
    }

    @Override
    public void insert(Tuple tuple) {
        internal.add(tuple);
    }

    @Override
    public void expire(long timestamp) {
        ListIterator<Tuple> iter = internal.listIterator();
        while (iter.hasNext()) {
            if (Math.abs(iter.next().getTimestamp() - timestamp) > size) {
                iter.remove();
            } else {
                break;
            }
        }
    }

    @Override
    public int size() {
        return internal.size();
    }
}
