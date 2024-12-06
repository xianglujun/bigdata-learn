package org.hadoop.learn.mp.fof;

import org.apache.hadoop.mapreduce.Partitioner;

public class FosSecondPhasePartitioner extends Partitioner<FofSecondPhaseDataModel, FofSecondPhaseDataModel> {
    @Override
    public int getPartition(FofSecondPhaseDataModel fofSecondPhaseDataModel, FofSecondPhaseDataModel fofSecondPhaseDataModel2, int numPartitions) {
        return (fofSecondPhaseDataModel.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
