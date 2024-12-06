package org.hadoop.learn.mp.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WordCountMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args == null || args.length == 0 || args.length != 2) {
            System.out.println("请输入输入/输出路径");
            return;
        }

        System.setProperty("HADOOP_HOME", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("hadoop.home.dir", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();

        // 设置本地运行
        configuration.set("mapreduce.framework.name", "local");

        JobConf jobConf = new JobConf(configuration);
        // 设置作业的输入输出路径
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(jobConf, outputPath);

        Job job = Job.getInstance(jobConf);

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(WordCountMain.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setJobName("wordcount");

        // 设置输出key的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置reducer的相关
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 提交作业并等待作业结束
        boolean b = job.waitForCompletion(true);

        System.out.println("任务是否执行完成: " + b);
    }
}
