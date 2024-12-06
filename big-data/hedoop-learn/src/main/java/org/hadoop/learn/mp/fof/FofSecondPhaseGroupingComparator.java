package org.hadoop.learn.mp.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FofSecondPhaseGroupingComparator extends WritableComparator {

    public FofSecondPhaseGroupingComparator() {
        super(FofSecondPhaseDataModel.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FofSecondPhaseDataModel w1 = (FofSecondPhaseDataModel) a;
        FofSecondPhaseDataModel w2 = (FofSecondPhaseDataModel) b;
        return w1.getUserId().compareTo(w2.getUserId());
    }
}
