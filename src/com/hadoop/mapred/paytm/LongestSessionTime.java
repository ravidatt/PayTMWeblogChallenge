/*
 * Author:Ravi Datt
 * Date : 06-Jan-2016
 */
package com.hadoop.mapred.paytm;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class LongestSessionTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text key = new Text();
	Text value = new Text();
	
	/*
	 * Split each record of data row using \" and " ";
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		String split1[] = ivalue.toString().split("\""); // 1st split by quotes ""
		String split2[] = split1[0].split(" ");// 2nd split by blank space " "
		String timestamp = split2[0]; // get the time stamp. Required*
		String clientip = split2[2]; // IP address of the client. Required*

		if (clientip.indexOf(":") != -1) { // remove port from ClientIP
			clientip = clientip.substring(0, clientip.indexOf(":"));
		}

		key.set("LONGEST_SESSION_TIME");// we require the same key so that we can have all IPs,TIMESTAMPs values to find out
										// average at reducer side.
		value.set(timestamp + "\t" + clientip);
		context.write(key, value);
	}
}

class LongestSessionTimeReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	Text value = new Text();
	Map<String, List<Long>> map = new HashMap<String, List<Long>>();

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		long longestTime = Long.MIN_VALUE;
		String longestTimeIp = null;
		for (String key : map.keySet()) {
			List<Long> timeList = (List<Long>) map.get(key);
			Long time = timeList.get(2);

			if (time > longestTime) {
				longestTimeIp = key;
				longestTime = time;
			}

		}

		Text key = new Text();
		DoubleWritable value = new DoubleWritable();
		// now finally push to HDFS longest total time
		key.set("LONGEST SESSION TIME (In Minutes) for IP " + longestTimeIp);
		value.set(longestTime / (1000 * 60));
		context.write(key, value);

	}

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text val : values) {
			String valStr[] = val.toString().split("\t");
			if (valStr != null && valStr.length > 0) {
				String timeStamp = valStr[0];
				Long pageHitTime = Instant.parse(timeStamp).toEpochMilli();
				String clientIP = valStr[1];
				if (map.containsKey(clientIP)) {
					List<Long> list = (List<Long>) map.get(clientIP);

					Long stTime = (Long) list.get(0);
					Long enTime = (Long) list.get(1);
					if (pageHitTime < stTime) {
						stTime = pageHitTime;
						list.set(0, pageHitTime);
					} else if (pageHitTime > enTime) {
						enTime = pageHitTime;
						list.set(1, pageHitTime);
					}
					Long elapsedTime = enTime - stTime;
					list.set(2, elapsedTime);

					map.put(clientIP, list);
				} else {
					List<Long> list = new ArrayList<Long>();
					list.add(pageHitTime);// startTime
					list.add(pageHitTime);// endTime
					list.add(0L);// Dummy elapsed Time to maintain consistency
					map.put(clientIP, list);
				}
			}
		}

	}

}

public class LongestSessionTime extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new LongestSessionTime(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// Job Name is "Longest Session Time"
		Job job = Job.getInstance(getConf(), "Longest Session Time");

		job.setJarByClass(com.hadoop.mapred.paytm.LongestSessionTime.class);
		// mapper class
		job.setMapperClass(LongestSessionTimeMapper.class);
		// reducer class
		job.setReducerClass(LongestSessionTimeReducer.class);
		// One Reducer
		job.setNumReduceTasks(1);

		// Set the Mapper output Key and Value data type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set Mapper Input and Output File Format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set final Key (Text) and Value (DoubleWritable) data type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// input and output location for Job
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
