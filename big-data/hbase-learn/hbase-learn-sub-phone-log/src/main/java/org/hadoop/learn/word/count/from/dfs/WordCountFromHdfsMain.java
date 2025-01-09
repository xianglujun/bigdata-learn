package org.hadoop.learn.word.count.from.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * 该类主要实现了从HDFS->MR->HBASE这样已过过程，实现了将HDFS的数据通过Reduce任务写出到HBASE中
 */
public class WordCountFromHdfsMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        System.setProperty("HADOOP_HOME", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("hadoop.home.dir", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration cfg = new Configuration(true);
        cfg.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        // 本地运行
        cfg.set("mapreduce.framework.name", "local");
        // 设置hdfs的FileSystem


        JobConf jobConf = new JobConf(cfg, WordCountFromHdfsMain.class);
        FileInputFormat.addInputPath(jobConf, new Path("/xianglujun/wordcount/wc.txt"));
        FileOutputFormat.setOutputPath(jobConf, new Path("/xianglujun/wordcount/wc_out_1231"));
        jobConf.setJobName("mp任务从hdfs中获取数据并存储到hbase");
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(LongWritable.class);

        TableMapReduceUtil.initTableReduceJob(
                "wordcount:wordcount", // 需要操作的表的名称
                WordCountFromHdfsReducer.class, // reduce任务
                jobConf,
                null, // 分区类
                false// 是否上传Hbase的包到分布式存储
        );

        jobConf.setJarByClass(WordCountFromHdfsMain.class);

        Job job = Job.getInstance(jobConf);
        job.setMapperClass(WordCountFromHdfsMapper.class);
        boolean completed = job.waitForCompletion(true);
        System.out.println("任务是否执行完成： " + completed);
    }
}
