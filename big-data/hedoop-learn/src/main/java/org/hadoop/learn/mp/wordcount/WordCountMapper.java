package org.hadoop.learn.mp.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第一个参数KEYIN: 读取文件的偏移量
 * 第二个参数VALUEIN: 代表了这一行的文本内容,输入的value类型
 * 第三个参数KEYOUT: 输出的key的value类型
 * 第四个擦拿书VALUEOUT 输出的value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable inKey, Text inValue, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        Thread.sleep(10000000);
        // 获取当前行文本内容
        String line = inValue.toString();
        // 按照空行进行拆分
        String[] words = line.split(" ");

        for (String word : words) {
            if (word.isEmpty()) {
                continue;
            }
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
