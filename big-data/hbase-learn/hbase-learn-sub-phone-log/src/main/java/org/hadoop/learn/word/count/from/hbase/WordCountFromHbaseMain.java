package org.hadoop.learn.word.count.from.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.hadoop.learn.word.count.from.dfs.WordCountFromHdfsMain;

import java.io.IOException;

public class WordCountFromHbaseMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_HOME", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("hadoop.home.dir", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration cfg = new Configuration(true);
        cfg.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        // 本地运行
        cfg.set("mapreduce.framework.name", "local");

        JobConf jobConf = new JobConf(cfg, WordCountFromHdfsMain.class);
//        FileInputFormat.addInputPath(jobConf, new Path("/xianglujun/wordcount/wc.txt"));
        FileOutputFormat.setOutputPath(jobConf, new Path("/xianglujun/wordcount/wc_out_1231"));
        jobConf.setJobName("mp任务从hdfs中获取数据并存储到hbase");
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(LongWritable.class);
        jobConf.set("mapred.mapper.new-api", "false");
        jobConf.set("mapreduce.map.compatibility.major.version", "false");

        //        jobConf.set("mapreduce.job.map.class", WordCountFromHbaseMapper.class.getName());
        TableMapReduceUtil.initTableMapJob(
                "/wordcount:sentences",
                "base:line",
                WordCountFromHbaseMapper.class,
                Text.class,
                LongWritable.class,
                jobConf,
                false
        );

        TableMapReduceUtil.initTableReduceJob(
                "wordcount:wordcount2", // 需要操作的表的名称
                WordCountFromHbaseReducer.class, // reduce任务
                jobConf,
                null, // 分区类
                false// 是否上传Hbase的包到分布式存储
        );

        jobConf.setJarByClass(WordCountFromHdfsMain.class);

        Job job = Job.getInstance(jobConf);
//        job.setMapperClass(WordCountFromHdfsMapper.class);
        new SentencesInitializer("wordcount", "sentences", cfg).init();
        boolean completed = job.waitForCompletion(true);
        System.out.println("任务是否执行完成： " + completed);
    }
}
