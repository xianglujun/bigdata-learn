package org.hadoop.learn.mp.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FofFirstPhaseReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int cnt = 0;
        boolean hasDirectReleation = false;
        for (IntWritable intWritable : values) {
            int relation = intWritable.get();
            if (relation == 0) {
                hasDirectReleation = true;
                // 直接的关系，不再推荐列表
                break;
            } else {
                // 间接的关系，则输出结果
                cnt ++;
            }
        }

        if (!hasDirectReleation) {
            context.write(key, new IntWritable(cnt));
        }

    }
}
