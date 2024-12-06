package org.hadoop.learn.mp.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FosFirstPhaseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable outVal = new IntWritable(1);
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\\s+");
        for (int i = 0; i < splits.length; i++) {
            String name = splits[i];

            int relation = 0;
            for (int j = i + 1; j < splits.length; j++) {
                String friend = splits[j];
                outVal.set(relation++ > 0 ? 1 : 0);
                outKey.set(getOutKeyName(name, friend));
                context.write(outKey, outVal);
            }
        }
    }

    private String getOutKeyName(String name, String friend) {
        return (name.compareTo(friend) > 0 ? name + ":" + friend : friend + ":" + name).toLowerCase();
    }
}
