/*
 * Author:Ravi Datt
 * Date : 06-Jan-2016
 */
package com.hadoop.mapred.paytm;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
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




class SessionMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text key = new Text();
	Text value = new Text();
	
	/*
	 * Split each record of data row using \" and " ";
	 * emit Key & Value to Reducer.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		String split1[] = ivalue.toString().split("\""); // 1st split by quotes ""
		String split2[] = split1[0].split(" ");// 2nd split by blank space " "
		String timestamp = split2[0]; // get the time stamp. Required*
		String clientip = split2[2]; // IP address of the client. Required*
		
		String request[]=split1[1].split(" ");// get the request String.
		
		String requestURL="";
		if(request.length>0){
		 requestURL = request[1];
		}
		
		if(clientip.indexOf(":")!=-1){
		 key.set(clientip.substring(0, clientip.indexOf(":"))); // remove the port from IP if it is there
		}
		else{
		 key.set(clientip);
		}
		value.set(timestamp+"\t"+requestURL);
		context.write(key, value);
	}

}

class SessionReducer extends Reducer<Text, Text, Text, Text> {
	
	Text value = new Text();
	int timeinminutes = 10; // default session time 10 minutes
	/*
	 * get SESSION_ACTIVE_TIME passed as part of Configuration from Job.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
    protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf =  context.getConfiguration();
		String time = (String)conf.get("SESSION_ACTIVE_TIME");
		timeinminutes = Integer.parseInt(time);
	}
    /*
     * 
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String output="";
		long startTime=0;
		long endTime = 0;
		
		/*
		 * TreeMap will store session time in order from session start to session end.
		 * Later get only those pages which have been hit with in 15 minutes of window from session start Time.
		 */
		
		Map<Long,String> map = new TreeMap<Long,String>();
		
		for (Text val : values) {
			String valStr[] = val.toString().split("\t");
			if(valStr!=null && valStr.length>0){
			String timeStamp = valStr[0];
			Instant instant = Instant.parse ( timeStamp );
			map.put(instant.toEpochMilli(), valStr[1]);
		  }
		}
		int timecount=-1;
		for(Long key:map.keySet()){
			
			if(timecount==-1){
				 startTime = key;
				 endTime = startTime+timeinminutes*60*1000;
			}
				
			if(key<=endTime){				
				  String url = map.get(key);
				  output=output+"\t"+url;
			}
			timecount++;
		}
		
		
		
		
		Date startDtTime=new Date(startTime);
		Date endDtTime=new Date(endTime);
		
		value.set("{"+startDtTime+","+endDtTime+"} \t ["+output+"] \n"); //  emit IP, starttime-endtime,URL and  to HDFS
		context.write(_key,value);
	}

}

public class Sessionize extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// set session time window.
	    conf.set("SESSION_ACTIVE_TIME","15");
		int res = ToolRunner.run(conf, new Sessionize(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		//Auto-generated method stub
		
		// Job Name is "Sessionzation"
		Job job = Job.getInstance(getConf(), "Sessionzation");
		
		job.setJarByClass(com.hadoop.mapred.paytm.Sessionize.class);
		// mapper class
		job.setMapperClass(SessionMapper.class);
		// reducer Class
		job.setReducerClass(SessionReducer.class);
		
		// One Reducer
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
