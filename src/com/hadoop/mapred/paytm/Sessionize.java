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
	
    
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		String logentry[] = ivalue.toString().split("\"");
		String split1[]=logentry[0].split(" ");
		String timestamp = split1[0];
//		String elb=split1[1];
		String client=split1[2];
//		String backend=split1[3];
//		String request_processing_time=split1[4];
//		String backend_processing_time=split1[5];
//		String response_processing_time=split1[6];
//		String elb_status_code=split1[7];
//		String backend_status_code=split1[8];
//		String received_bytes=split1[9];
//		String sent_bytes=split1[10];
		String request[]=logentry[1].split(" ");
		//String user_agent=logentry[2];
		String requestURL="";
		if(request.length>0){
		 requestURL = request[1];
		}
		
		if(client.indexOf(":")!=-1){
		 key.set(client.substring(0, client.indexOf(":")));
		}
		else{
		 key.set(client);
		}
		value.set(timestamp+"\t"+requestURL);
		context.write(key, value);
	}

}

class SessionReducer extends Reducer<Text, Text, Text, Text> {
	
	
    
	Text value = new Text();
	int timeinminutes = 10; // default session time 10 minutes
	
    protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf =  context.getConfiguration();
		String time = (String)conf.get("SESSION_ACTIVE_TIME");
		timeinminutes = Integer.parseInt(time);
	}
    
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String output="";
		long startTime=0;
		long endTime = 0;
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
		
		value.set("{"+startDtTime+","+endDtTime+"} , ["+output+"] \n");
		context.write(_key,value);
	}

}

public class Sessionize extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    conf.set("SESSION_ACTIVE_TIME","15");
		int res = ToolRunner.run(conf, new Sessionize(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		//Auto-generated method stub
		
		Job job = Job.getInstance(getConf(), "Sessionzation");
		
		job.setJarByClass(com.hadoop.mapred.paytm.Sessionize.class);
		// mapper
		job.setMapperClass(SessionMapper.class);
		// reducer
		job.setReducerClass(SessionReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	
	}

}
