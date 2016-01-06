Paytm Weblog Challenge

Mapreduce:


	1) jar file name : paytm.jar ( this file has Mapper, Reducer, Driver classes as well PIG UDF functions classes)

	2) Data File Name: weblog.zip

	3) create new dirs in HDFS and Move data file to HDFS using following linux commands: 

		A) hdfs dfs -mkdir /user
		B) hdfs dfs -mkdir /user/paytm
		C) hdfs dfs -put /user/hduser/weblog.log /user/paytm/weblog.log


	4) Execute mapreduce jobs using:

	i)  hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.LongestSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/  
	    LongestSessionTime.dat
	ii) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.UniqueURLVisit /user/paytm/weblog.log /user/paytm/MRoutput/	
	    UniqueURLVisit.dat
	iii)hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.AverageSessionTime /user/paytm/weblog.log /user/paytm/MRoutput/	
	    AverageSessionTime.dat
	iv) hadoop jar paytm.jar com.hadoop.mapred.paytm.SessionMapper.Sessionize /user/paytm/weblog.log /user/paytm/MRoutput/Sessionize.dat


	5) out put Files (after running MR jobs) can be found under MRoutput folder:

	i) LongestSessionTime.dat
	ii) UniqueURLVisit.dat   
	iii) AverageSessionTime.dat   
	iv) Sessionize.dat


Pig Script file:

	File Name: webLogPigScript.pig



