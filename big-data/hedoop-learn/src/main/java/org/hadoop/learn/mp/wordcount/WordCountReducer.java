package org.hadoop.learn.mp.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // 定义当前单词出现的总次数
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }

        context.write(key, new LongWritable(sum));
    }
}
