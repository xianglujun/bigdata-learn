package org.hadoop.learn.mp.temporary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.hadoop.learn.mp.wordcount.WordCountReducer;

import java.io.IOException;

public class WeatherMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args == null || args.length == 0 || args.length != 2) {
            System.out.println("请输入输入/输出路径");
            return;
        }

        System.setProperty("HADOOP_HOME", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("hadoop.home.dir", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration cnf = new Configuration();

        // 设置本地运行
//        cnf.set("mapreduce.framework.name", "local");

        JobConf jobConf = new JobConf(cnf);
        // 设置作业的输入输出路径
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(jobConf, outputPath);

        Job job = Job.getInstance(jobConf);

        FileSystem fs = FileSystem.get(cnf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(WeatherMain.class);
        job.setMapperClass(WeatherMapper.class);
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);

        job.setReducerClass(WordCountReducer.class);
        job.setJobName("weather_comparator");
        job.setSortComparatorClass(WeatherSortComparator.class);

        // 设置输出key的类型
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(Weather.class);
        job.setPartitionerClass(WeatherPartitioner.class);

        // 设置reducer的相关
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(WeatherReducer.class);
        // 设置4个reducer任务，也决定了分区的数量
        job.setNumReduceTasks(4);

        // 提交作业并等待作业结束
        boolean b = job.waitForCompletion(true);

        System.out.println("任务是否执行完成: " + b);
    }
}
