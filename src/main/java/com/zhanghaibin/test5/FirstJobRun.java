package com.zhanghaibin.test5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhanghaibin.test4.TowJob;
import com.zhanghaibin.test4.TowMapper;
import com.zhanghaibin.test4.TowReduce;

public class FirstJobRun {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.jobtracker.address", "CHDNode1:9001");
		conf.set("fs.defaultFS", "hdfs://cdhnode1:9000");
		conf.set("date", "2015-04-26");
		conf.set("timepoint", "09-17-24");
		System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.7.3");
		
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("yidong");
			job.setJarByClass(FirstJobRun.class);
			
			job.setMapperClass(FirstMap.class);
			job.setReducerClass(FirstReduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path("/output/yidong"));
			FileOutputFormat.setOutputPath(job, new Path("/output/yidong"));
			//System.exit(job.waitForCompletion(true) ? 0 : 1);
			boolean f = job.waitForCompletion(true);
			if(f) {
				System.out.println("执行FirstJob成功");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
