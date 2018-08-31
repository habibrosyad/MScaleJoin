package mscalejoin.shj;

import mscalejoin.common.Probe;
import mscalejoin.common.Stream;

public class ProbeImpl implements Probe {
    private final Stream target;
    private final int targetAttribute, sourceAttribute;

    public ProbeImpl(Stream target, int targetAttribute, int sourceAttribute) {
        this.target = target;
        this.targetAttribute = targetAttribute;
        this.sourceAttribute = sourceAttribute;
    }

    @Override
    public Stream getTarget() {
        return target;
    }

    int getTargetAttribute() {
        return targetAttribute;
    }

    int getSourceAttribute() {
        return sourceAttribute;
    }
}
