package org.hadoop.learn.mp.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 对第一阶段产生的结果进行处理：user:friend 1
 */
public class FofSecondPhaseMapper extends Mapper<LongWritable, Text, FofSecondPhaseDataModel, FofSecondPhaseDataModel> {

    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FofSecondPhaseDataModel, FofSecondPhaseDataModel>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] splits = line.split("\\s+");
        Integer cnt = Integer.valueOf(splits[1]);
        String[] relations = splits[0].split(":");

        FofSecondPhaseDataModel dataModel = new FofSecondPhaseDataModel();

        dataModel.setUserId(relations[0]);
        dataModel.setFriendId(relations[1]);
        dataModel.setCnt(cnt);

        context.write(dataModel, dataModel);
    }
}
