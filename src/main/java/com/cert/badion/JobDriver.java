package com.cert.badion;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class JobDriver extends Configured implements Tool{

	private final Logger log = Logger.getLogger(JobDriver.class);
	
	public static void main(String[] args) {
		Configuration defaultConf = new Configuration();
		defaultConf.set("mapred.textoutputformat.separator", ",");

		int res;
		try {
			res = ToolRunner.run(defaultConf, new JobDriver(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public int run(String[] args) throws Exception {
		String input = "/home/cloudera/badion/cert/2008.csv";
		String output = "/home/cloudera/badion/cert/output/";
		String weather = "/home/cloudera/badion/cert/weather.csv";
		
		log.info("INPUT PATH: " + input);
		log.info("OUTPUT PATH: " + output);
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance();
		job.setNumReduceTasks(2);
		job.addCacheFile(new URI(weather));
		job.setJarByClass(JobDriver.class);
		
		job.setInputFormatClass(FlightInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FlightInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setMapperClass(FlightMapper.class);
		
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setReducerClass(FlightReducer.class);
		
		
		job.setOutputKeyClass(FlightDataModel.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(MonthPartitioner.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
