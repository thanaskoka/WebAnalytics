# WebAnalytics
Master Thesis on Big Data Web Analytics: Integrating Ifml models with Server Logs And Runtime Logs to detect User Behaviour


## Packages Content
* **com.webanalytics.analysis**: contains the scala classes with the Log Enrichment Phase and the Analysis phase 
	* LogEnrichment
	* LogAnalysis
	
## Compile Project

* Install [sbt](http://www.scala-sbt.org)
* cd \<project folder>
* sbt assembly

The assembly action creates a "fat-jar" containing all the needed dependencies


## Run Spark Application

In the Folder SparkApplication there is the fat-jar compiled in the previous step.

###Copy the jar in the head node of the cluster with the following command:
* scp <local-origin-path> <ssh-user>@<cluster-name>-ssh.azurehdinsight.net:<destination-path>
* ssh-user is the user that was created at the cluster creation moment
	
###connect to the cluster with the ssh username   trough an ssh protocol (if Windows is used you need to use putty, for mac you have ssh shell by default)

###Run the spark app 
*execute the spark Application with the following command

/usr/hdp/current/spark-client/bin/spark-submit \
--class com.webanalytics.analysis.LogEnrichment \
--name WebAnalytics \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 5G \
--executor-cores 5 \
--num-executors 3 \
WebAnalytics.jar \
"ContainerName" "BlobStorageName" "WebModelPath" "DataModelPath" "DbPath" "ApacheLogPath" "RtxLogPath" "outputAnalysisPath"

**you need to pass to the jar Application 8 parameter to work properly:

*ContainerName  :the container name of the blob storage used to analyze the data
*BlobStorageName  :the Blob Storage name used to analyze the data
*WebModelPath  :the Relative Path of the Container where is saved the WebModel
*DataModelPath  :the Relative Path of the Container where is saved the WebModel
*DbPath  :the Relative Path of the Container where is saved the DB dump
*ApacheLogPath  :the Relative Path of the Container where are saved the Apache Log to be analyzed
*RtxLogPath  :the Relative Path of the Container where are saved the Rtx Log to be analyzed
*outputAnalysisPath  :the Relative Path of the Container where you want to save the final Analysis 

##An example of the 8 parameters can be: 
*"provaContainer" "tesithanas" "data/WebModel/" "data/DataModel/" "data/DbIstance/" "data/dataset-20161216/" "data/dataset-20161216/" "data/OutputhAnalysis/"






