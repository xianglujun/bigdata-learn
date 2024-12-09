package org.hadoop.learn.mp.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class FofSecondPhaseMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args == null || args.length == 0 || args.length != 2) {
            System.out.println("请输入输入/输出路径");
            return;
        }

        System.setProperty("HADOOP_HOME", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("hadoop.home.dir", "H:\\xianglujun\\hadoop-2.6.5");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration cnf = new Configuration();

        // todo 调试使用，在正式环境需要注释:设置本地运行
        cnf.set("mapreduce.framework.name", "local");

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

        job.setJarByClass(FofSecondPhaseMain.class);
        job.setMapperClass(FofSecondPhaseMapper.class);

        job.setReducerClass(FofSecondPhaseReducer.class);
        job.setJobName("好友推荐第二个阶段..");

        // 设置输出key的类型
        job.setMapOutputKeyClass(FofSecondPhaseDataModel.class);
        job.setMapOutputValueClass(FofSecondPhaseDataModel.class);
        job.setGroupingComparatorClass(FofSecondPhaseGroupingComparator.class);
        job.setPartitionerClass(FosSecondPhasePartitioner.class);

        // 设置reducer的相关
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(FofSecondPhaseSortComparator.class);
        job.setPartitionerClass(FosSecondPhasePartitioner.class);
        // todo 当mapper和reduce的输出key和value都相同时，对于reducer的设置可以省略

        // 提交作业并等待作业结束
        boolean b = job.waitForCompletion(true);

        System.out.println("任务是否执行完成: " + b);
    }
}
