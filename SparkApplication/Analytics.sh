#!/bin/sh
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