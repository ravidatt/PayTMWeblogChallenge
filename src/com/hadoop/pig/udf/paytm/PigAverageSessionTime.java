/*
 * Author:Ravi Datt
 * Date : 06-Jan-2016
 */
package com.hadoop.pig.udf.paytm;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class PigAverageSessionTime extends EvalFunc<Long> {

	/*
	 * Get the DataBag from Tuple and collect pageHit startTime and endTime for each IP.
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Long exec(Tuple input) throws IOException {
		
		if (input == null || input.size() == 0){
			 return null;
		}
		
		DataBag bag = (DataBag)input.get(1);
		Iterator<Tuple> it = bag.iterator();
		List<Long> list = new ArrayList<Long>();
		Long startTime = 0L;
		Long endTime = 0L;
		Long totalsessionTime=0L;


		while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            String timeStamp = (String)t.get(0);
            Instant instant = Instant.parse ( timeStamp );
            list.add(instant.toEpochMilli());
		}
		
		Collections.sort(list); // sort the list for date & time ascending order.
		startTime = list.get(0); // Get the session start datetime
		endTime = list.get(list.size()-1);// Get the session end datetime
		
		totalsessionTime=endTime-startTime;
		return totalsessionTime;
		
	}

}
