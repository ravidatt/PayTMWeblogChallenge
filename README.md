Paytm Weblog Challenge

<h2> Map Reduce </h2>:


	1) <b> jar file name : paytm.jar ( this file has Mapper, Reducer, Driver classes as well PIG UDF functions classes) </b>

	2) <b> Data File Name: weblog.zip </b>

	3) <b> create new dirs in HDFS and Move data file to HDFS using following linux commands: </b>

		A) hdfs dfs -mkdir /user
		B) hdfs dfs -mkdir /user/paytm
		C) hdfs dfs -put /user/hduser/weblog.log /user/paytm/weblog.log


	4) <b> Execute mapreduce jobs using: </b>

	i)  hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.LongestSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/  
	    LongestSessionTime.dat
	ii) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.UniqueURLVisit /user/paytm/weblog.log /user/paytm/MRoutput/	
	    UniqueURLVisit.dat
	iii)hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.AverageSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/	
	    AverageSessionTime.dat
	iv) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.Sessionize /user/paytm/weblog.log /user/paytm/MRoutput/Sessionize.dat


	5) <b> out put Files (after running MR jobs) can be found under MRoutput folder: </b>

	i) LongestSessionTime.dat
	ii) UniqueURLVisit.dat   
	iii) AverageSessionTime.dat   
	iv) Sessionize.dat

	6) <b> Source File: PayTMWeblogChallenge-src.zip </b>


<h2> Pig </h2>:

	i) Pig Script File : /pigScript/webLogPigScript.pig

	ii) Pig output File : /pigoutput/hduser/



