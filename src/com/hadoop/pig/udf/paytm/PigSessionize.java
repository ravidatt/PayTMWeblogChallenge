package com.hadoop.pig.udf.paytm;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class PigSessionize extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0){
			 return null;
		}
		
			try{
			
			Map<Long,String> map = new TreeMap<Long,String>();
			String output="";
			long startTime=0;
			long endTime = 0;
			int timeinminutes = 15;
			DataBag bag = (DataBag)input.get(1);
			Iterator<Tuple> it = bag.iterator();
	
			while (it.hasNext()){
	            Tuple t = (Tuple)it.next();
	            String timeStamp = (String)t.get(0);
	            String url = (String)t.get(2);
	            Instant instant = Instant.parse ( timeStamp );
				map.put(instant.toEpochMilli(), url);
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
			return "{"+startDtTime+","+endDtTime+"} , ["+output+"]";

			}catch(Exception e){
			throw new IOException("Caught exception processing input row ", e);
			}
	}
}
