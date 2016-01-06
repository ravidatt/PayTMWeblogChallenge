/*
 * Author:Ravi Datt
 * Date : 06-Jan-2016
 */
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

	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0){
			 return null;
		}
		
			try{
			
			/*
			 * TreeMap will store session time in order from session start to session end.
			 * Later get only those pages which have been hit with in 15 minutes of window from session start Time.
			 */
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
				
				if(timecount==-1){// on very first iteration get the starttime and also set endTime
					 startTime = key;
					 endTime = startTime+timeinminutes*60*1000; // session starttime  + 15 minutes.
				}
					
				if(key<=endTime){// Only interested in those pages which haven been hit with in start and end time window.			
					  String url = map.get(key);
					  output=output+"\t"+url;
				}
				timecount++;
			}
			
			Date startDtTime=new Date(startTime);
			Date endDtTime=new Date(endTime);
			return "{"+startDtTime+","+endDtTime+"} \t ["+output+"]";

			}catch(Exception e){
			throw new IOException("Caught exception processing input row ", e);
			}
	}
}
