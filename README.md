# WebAnalytics
Master Thesis on Big Data Web Analytics: Integrating Ifml models with Server Logs And Runtime Logs to detect User Behaviour


## Packages Content
* **com.webanalytics.analysis**: contains the scala classes with the Log Enrichment Phase and the Analysis phase 
	* LogEnrichment
	* LogAnalysis
	
* **com.webanalytics.config**: contains the scala trait that configure the DataSource Path(Blob container+Relative Path) and the intervals of the Analysis
	* DataPreparation
* **com.webanalytics.config**: contains the scala functions that parse,transform and process the data
	* Utilities
	
## Compile Project

* Install [sbt](http://www.scala-sbt.org)
* cd \<project folder>
* sbt assembly

The assembly action creates a "fat-jar" containing all the needed dependencies


## Run Spark Application

In the Folder SparkApplication there is the fat-jar compiled in the previous step and an example of the configuration file to be passed as parameter to the jar application.

###1) Copy the jar in the head node of the cluster with the following command:
* scp <"local-origin-path"> <"ssh-user">@<"cluster-name">-ssh.azurehdinsight.net:<"destination-path">
 
 
 ssh-user is the user that was created at the cluster creation moment
	
###2) connect to the cluster with the ssh username   trough an ssh protocol (if Windows is used you need to use putty, for mac you have ssh shell by default in your terminal)
* ssh <"ssh-user">@<"cluster-name">-ssh.azurehdinsight.net (Mac example)

###3) Run the spark app 
* go to the destination path in the cluster and execute the spark Application with the following command(you can find the command in the folder SparkApplication/Analytics.sh

/usr/hdp/current/spark-client/bin/spark-submit \
--class com.webanalytics.analysis.LogAnalysis \
--name WebAnalytics \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 5G \
--executor-cores 5 \
--num-executors 3 \
WebAnalytics.jar \
"wasb://dataset@demowebanalytics.blob.core.windows.net/AnalysisConfiguration.conf"

** you need to pass to the jar Application as parameter the configuration file path to work properly:


