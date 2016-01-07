Paytm Weblog Challenge

<h2> Mapreduce </h2>:


	1) <h3> jar file name : paytm.jar ( this file has Mapper, Reducer, Driver classes as well PIG UDF functions classes) </h3>

	2) <h3> Data File Name: weblog.zip </h3>

	3) <h3> create new dirs in HDFS and Move data file to HDFS using following linux commands: </h3>

		A) hdfs dfs -mkdir /user
		B) hdfs dfs -mkdir /user/paytm
		C) hdfs dfs -put /user/hduser/weblog.log /user/paytm/weblog.log


	4) <h3> Execute mapreduce jobs using: </h3>

	i)  hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.LongestSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/  
	    LongestSessionTime.dat
	ii) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.UniqueURLVisit /user/paytm/weblog.log /user/paytm/MRoutput/	
	    UniqueURLVisit.dat
	iii)hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.AverageSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/	
	    AverageSessionTime.dat
	iv) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.Sessionize /user/paytm/weblog.log /user/paytm/MRoutput/Sessionize.dat


	5) <h3> out put Files (after running MR jobs) can be found under MRoutput folder: </h3>

	i) LongestSessionTime.dat
	ii) UniqueURLVisit.dat   
	iii) AverageSessionTime.dat   
	iv) Sessionize.dat

	6) <h3> Source File: PayTMWeblogChallenge-src.zip </h3>


<h2> Pig </h2>:

	i) Pig Script File : /pigScript/webLogPigScript.pig

	ii) Pig output File : /pigoutput/hduser/



