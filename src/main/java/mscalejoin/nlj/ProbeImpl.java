package mscalejoin.nlj;

import mscalejoin.common.Probe;
import mscalejoin.common.Stream;

public class ProbeImpl implements Probe {
    private final Stream target;
    private final Predicate predicate;

    public ProbeImpl(Stream target, Predicate predicate) {
        this.target = target;
        this.predicate = predicate;
    }

    @Override
    public Stream getTarget() {
        return target;
    }

    Predicate getPredicate() {
        return predicate;
    }
}
