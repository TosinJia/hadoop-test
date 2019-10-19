package com.tosin.hadoop.mapreduce.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 7.9 找博客共同好友案例
 */
public class AllCommonFriendsDriver {
    public static void main(String[] args) throws IOException, InterruptedException {
        // OneCommonFriendsDriver
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        job1.setJarByClass(OneCommonFriendsDriver.class);
        job1.setMapperClass(OneCommonFriendsMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(OneCommonFriendsReducer.class);
        job1.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        //TwoCommonFriendsDriver
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(TwoCommonFriendsDriver.class);

        job2.setMapperClass(TwoCommonFriendsMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(TwoCommonFriendsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Job控制器
        JobControl jobControl = new JobControl("Tosin");

        ControlledJob aJob = new ControlledJob(job1.getConfiguration());
        ControlledJob bJob = new ControlledJob(job2.getConfiguration());
        // bJob添加依赖job aJob
        bJob.addDependingJob(aJob);
        // job控制器添加job
        jobControl.addJob(aJob);
        jobControl.addJob(bJob);

        Thread thread = new Thread(jobControl);
        thread.start();

        while(!jobControl.allFinished()){
            Thread.sleep(1000);
        }
        System.out.println("Over");

    }
}
