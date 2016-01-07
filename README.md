Paytm Weblog Challenge

<h2> Map Reduce </h2>


	1)  jar file name : paytm.jar ( this file has Mapper, Reducer, Driver classes as well PIG UDF functions classes) 

	2)  Original Data File Name: weblog.zip 

	3)  To create new dirs in HDFS and can move data file (weblog.log in weblog.zip file) to HDFS using following hadoop based commands: 

		A) hdfs dfs -mkdir /user
		B) hdfs dfs -mkdir /user/paytm
		C) hdfs dfs -put /user/hduser/weblog.log /user/paytm/weblog.log


	4)  To execute 4 different mapreduce jobs using: 

	i)  hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.LongestSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/  
	    LongestSessionTime.dat
	ii) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.UniqueURLVisit /user/paytm/weblog.log /user/paytm/MRoutput/	
	    UniqueURLVisit.dat
	iii)hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.AverageSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/	
	    AverageSessionTime.dat
	iv) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.Sessionize /user/paytm/weblog.log /user/paytm/MRoutput/Sessionize.dat


	5)  Executed MR jobs's output Files can be found under MRoutput folder: 

	i) LongestSessionTime.dat
	ii) UniqueURLVisit.dat   
	iii) AverageSessionTime.dat   
	iv) Sessionize.dat

	6)  Source Files: PayTMWeblogChallenge-src.zip 


<h2> Pig </h2>

	i) Pig Script File : /pigScript/webLogPigScript.pig

	ii) Pig output File : /pigoutput/hduser/



