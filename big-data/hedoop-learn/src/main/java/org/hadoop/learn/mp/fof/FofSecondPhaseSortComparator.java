package org.hadoop.learn.mp.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FofSecondPhaseSortComparator extends WritableComparator {

    public FofSecondPhaseSortComparator() {
        super(FofSecondPhaseDataModel.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FofSecondPhaseDataModel w1 = (FofSecondPhaseDataModel) a;
        FofSecondPhaseDataModel w2 = (FofSecondPhaseDataModel) b;

        int cmp = w1.getUserId().compareTo(w2.getUserId());
        return cmp == 0 ? -w1.getCnt().compareTo(w2.getCnt()) : cmp;
    }
}