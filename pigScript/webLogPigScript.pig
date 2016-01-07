/*
 ********** create pig schema for weblog data  *************
*/

       weblog = LOAD '/user/paytm/weblog.log' USING  PigStorage(' ') as (timezone:chararray,elb:chararray,client:chararray,backend:chararray, request_processing_time:double,backend_processing_time:double,response_processing_time:double,elb_status_code:int,backend_status_code:int,received_bytes:int,sent_bytes:int,request_get:chararray,request_url:chararray,user_agent:chararray,ssl_cipher:chararray,ssl_protocol:chararray);

-- register paytm.jar file where UDF functions are written to process weblog data.

       REGISTER /home/hduser/workspace/PayTMWeblogChallenge/paytm.jar;

/*
   ************* get Timezone,clientIP and request URL into  WEBLOG_TIMEZONE_IP_REQUEST *************
*/

       WEBLOG_TIMEZONE_IP_REQUEST = FOREACH weblog GENERATE timezone, org.apache.pig.piggybank.evaluation.string.SUBSTRING(client,0,org.apache.pig.piggybank.evaluation.string.INDEXOF(client,':')) as IP, request_url;

/*
   ************ group WEBLOG_TIMEZONE_IP_REQUEST by client IP ****************
*/

        GROUPED_WEBLOG_TIMEZONE_IP_REQUEST = group WEBLOG_TIMEZONE_IP_REQUEST by IP;





/*
  "Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window."

  1) Now load first 15 minutes of session data from GROUPED_WEBLOG_TIMEZONE_IP_REQUEST
     by using UDF function PigSessionize which is part of paytm.jar which is loaded earlier in line number 9 above.
  2) Store final output into FINAL_HIT_BY_SESSION_TIME_15_MIN file and push to HDFS
*/
       HIT_BY_SESSION_TIME_15_MIN = FOREACH GROUPED_WEBLOG_TIMEZONE_IP_REQUEST generate group,com.hadoop.pig.udf.paytm.PigSessionize(*);

       store HIT_BY_SESSION_TIME_15_MIN into 'FINAL_HIT_BY_SESSION_TIME_15_MIN';




/*
    "Determine the average session time"

    1) Load the total session time group by each individual client IP.
    2) Group all session time together to find out total average of all sessions.
    3) Load the average session time among all total sessions.
    4) Store final output into file FINAL_AVG_SESSION_TIME and push to HDFS
*/

        session_time_for_ip = FOREACH GROUPED_WEBLOG_TIMEZONE_IP_REQUEST generate group,com.hadoop.pig.udf.paytm.PigAverageSessionTime(*)/60000 as sessionTime;
        GROUP_ALL = group session_time_for_ip All;

     -- one way to find out average
         AVG_SESSION_TIME = foreach GROUP_ALL {
         sum = SUM(session_time_for_ip.sessionTime);
         count = COUNT(session_time_for_ip);
         generate (sum/count) as AVERAGE_TIME_IN_MINUTES;
     };

     -- one more way to find out average
         AVG_SESSION_TIME = FOREACH GROUP_ALL GENERATE AVG(session_time_for_ip.sessionTime) as AVERAGE_TIME_IN_MINUTES;

         store AVG_SESSION_TIME into 'FINAL_AVG_SESSION_TIME';




/*
   "Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session."

   1) Group session by IP and get unique URL from each group.
   2) Group unique URL BY IP.
   3) Store final output into file FINAL_UNIQUE_URL_VISITED_BY_IP and push to HDFS
*/

         UNIQUE_URL_GROUP_BY_IP =
             FOREACH (group WEBLOG_TIMEZONE_IP_REQUEST by IP) {
                 b = WEBLOG_TIMEZONE_IP_REQUEST.request_url;
                 s = DISTINCT b;
                 GENERATE FLATTEN(s), group as IP;
             };

         UNIQUE_URL_VISITED_BY_IP = group UNIQUE_URL_GROUP_BY_IP by IP;

         store UNIQUE_URL_VISITED_BY_IP into 'FINAL_UNIQUE_URL_VISITED_BY_IP';



/*
   "Find the most engaged users, ie the IPs with the longest session times"

   1) Find out session start time and end time in millisec.
   2) Order sessions from most engaged ip(user) to least engaged ip(user)
   3) Get top most engaged user;
   4) Store final output into file FINAL_LONGEST_SESSION and push to HDFS
*/

	WEBLOG_TIMEZONE_IP_REQUEST = FOREACH weblog GENERATE ToMilliSeconds(ToDate(timezone)) AS (dttime:long), 	
	org.apache.pig.piggybank.evaluation.string.SUBSTRING		
	(client,0,org.apache.pig.piggybank.evaluation.string.INDEXOF(client,':')) as IP, request_url;


         SESSION_TIME =
             FOREACH (group WEBLOG_TIMEZONE_IP_REQUEST by IP) {
                 ma = MAX(WEBLOG_TIMEZONE_IP_REQUEST.dttime);
                 mi = MIN(WEBLOG_TIMEZONE_IP_REQUEST.dttime);
                 GENERATE (ma-mi) as SessionTime,group as IP;
             };


         SESSION_ORDER = ORDER SESSION_TIME BY SessionTime DESC; 
         LONGEST_SESSION = LIMIT SESSION_ORDER 1;
         store LONGEST_SESSION into 'FINAL_LONGEST_SESSION';


