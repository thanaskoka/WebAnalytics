#!/bin/sh
/usr/hdp/current/spark-client/bin/spark-submit \
--class com.webanalytics.analysis.LogAnalysis \
--name WebAnalytics \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 10G \
--executor-cores 5 \
--num-executors 3 \
WebAnalytics.jar \
"wasb://tesi@datasettesi.blob.core.windows.net/data/AnalysisConfiguration.conf"