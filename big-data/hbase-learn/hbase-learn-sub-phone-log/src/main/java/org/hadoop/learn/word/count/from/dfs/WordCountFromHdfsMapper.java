package org.hadoop.learn.word.count.from.dfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 用于对输入的拆分，将句子拆分成单词等
 */
public class WordCountFromHdfsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (null == line || line.trim().equals("")) {
            return;
        }

        String[] words = line.split(" ");
        for (String word : words) {
            String s = word.toLowerCase().replaceAll("![a-z]]", "");
            outKey.set(s);
            context.write(outKey, outValue);
        }
    }
}
