/*
 * Author:Ravi Datt
 * Date : 06-Jan-2016
 */
package com.hadoop.mapred.paytm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

class UniqueURLVisitMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text key = new Text();
	Text value = new Text();
	
	/*
	 * Split each record of data row using \" and " ";
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {		
		String logentry[] = ivalue.toString().split("\"");
		String split1[]=logentry[0].split(" ");
		String client=split1[2];
		String request[]=logentry[1].split(" ");
		String requestURL="";
		if(request.length>0){
		 requestURL = request[1];
		}
		/*
		 * Set IP as key and URL as value
		 */
		if(client.indexOf(":")!=-1){
		 key.set(client.substring(0, client.indexOf(":"))); // remove the port from IP if there
		}
		else{
		 key.set(client);
		}
		value.set(requestURL);
		context.write(key, value);
	}
}

class UniqueURLVisitReducer extends Reducer<Text, Text, Text, Text> {

	Text value = new Text();
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<String,List<String>> map = new TreeMap<String,List<String>>();
		/*
		 * Get all values (URL) per Key(IP)
		 */
		for (Text val : values) {
			if(map.containsKey(_key.toString())){
				List<String> list = (List<String>)map.get(_key.toString());
				if(!list.contains(val.toString())){ // Ignore if URL already exists in List to remove redundancy.
				  list.add(val.toString());
				  map.put(_key.toString(),list);
				}
			}
			else{				
				List<String> list = new ArrayList<String>();
				list.add(val.toString());
				map.put(_key.toString(),list);
			}
		}
		value.set(((List<String>)map.get(_key.toString())).toString()); // emit IP and unique URLs to HDFS.
		context.write(_key, value);
		
	}

}

public class UniqueURLVisit extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new UniqueURLVisit(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		//Auto-generated method stub
		
		Job job = Job.getInstance(getConf(), "Unique URL Visit");
		
		job.setJarByClass(com.hadoop.mapred.paytm.UniqueURLVisit.class);
		// mapper class
		job.setMapperClass(UniqueURLVisitMapper.class);
		// reducer class
		job.setReducerClass(UniqueURLVisitReducer.class);
		
		// one reducer
		job.setNumReduceTasks(1);
		
		// Set the Mapper output Key and Value data type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set Mapper Input and Output File Format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set final Key (Text) and Value (Text) data type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// input and output location for Job
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	
	}

}
