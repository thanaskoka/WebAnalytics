package com.webanalytics.analysis

import com.webanalytics.config._
import com.webanalytics.helper._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
/**
  * Created by Thanas Koka on 21/02/2017.
  */
object LogEnrichment extends DataPreparation {


  def main(args: Array[String]): Unit = {



  //run as spark standalone mode
    val conf= new SparkConf().setAppName("WebAnalytics").setMaster("local")
    val sc = new SparkContext(conf)

   //  val sc = new SparkContext()

    val sqlContext = new SQLContext(sc)

    readParameters(args,sqlContext)
    //Parse Models,Logs and Read from Db

    val ParsedDataModel = Utilities.parseDataModel(sqlContext)
    val ParsedPageWebModel=Utilities.parsePagesWebModel(sqlContext)
    val ParsedLink=Utilities.parseLink(sqlContext)
    val Dataframes=DbNames.map(line=>Utilities.parseDbIstance(line,sqlContext)).filter(l=>l!=null)
    val ParsedWebModel=Utilities.parseViewComponent(sqlContext)
    val PartialApacheLog=Utilities.parseApacheLogs(sc,sqlContext,ParsedDataModel,ParsedPageWebModel)
    val EnrichedLog=Utilities.parseRtxLogs(sc,sqlContext,ParsedPageWebModel)


    //combine Web model and Data Model
    val CombinedModels=ParsedWebModel.join(ParsedDataModel, (ParsedWebModel("Entity") === ParsedDataModel("EntityId")))
    val AddDisplayableAtt_to_CombinedModels=CombinedModels.withColumn("AttributeDisplayable",when(CombinedModels("DisplayedAttribute") ===CombinedModels("AttributeId"), true).otherwise(false))
    val parsedCombinedModels=AddDisplayableAtt_to_CombinedModels.where("AttributeDisplayable =true").drop("AttributeDisplayable")

    //Enrich Rtx Logs
    val EnrichedRtx=EnrichedLog.join(parsedCombinedModels,EnrichedLog("unit")===parsedCombinedModels("UnitId"))
    EnrichedRtx.registerTempTable("EnrichedRtx")

    //Enrich Apache Logs
    val ApacheLogandLinks=Utilities.enrichApacheLogs(sqlContext,PartialApacheLog,parsedCombinedModels,ParsedLink)

    //get Oid Value from DB
    val OidDB=Utilities.addOidValueDb(sc,sqlContext)

    //add oid value to Rtx Logs
    val RtxWithOidDB= EnrichedRtx.join(OidDB, (EnrichedRtx("oid")===OidDB("oidwithDBvalue")) && (EnrichedRtx("TableDB")===OidDB("tableNamewithDBvalue")) &&   EnrichedRtx("AttributeDatabaseColumn")===OidDB("tableColumnwithDBvalue"),"left_outer").withColumnRenamed("Time","TimeRtxLog").withColumnRenamed("sessionId","sessionIdRtx").cache()
    RtxWithOidDB.registerTempTable("RtxWithOidDB")

    //add oid value to Apache Logs
    val ApacheWithOidDB= ApacheLogandLinks.join(OidDB, (ApacheLogandLinks("OidValue")===OidDB("oidwithDBvalue")) && (ApacheLogandLinks("TableDB")===OidDB("tableNamewithDBvalue")) &&   ApacheLogandLinks("AttributeDatabaseColumn")===OidDB("tableColumnwithDBvalue"),"left_outer").drop("TableDB").drop("oidwithDBvalue").drop("tableNamewithDBvalue").drop("AttributeDatabaseColumn").withColumnRenamed("tableColumnwithDBvalue","ClickedTypeOid").withColumnRenamed("oidDBvalue","ClickedOidDBvalue").dropDuplicates()//.cache()//.orderBy(asc("Time")).cache()
    ApacheWithOidDB.registerTempTable("ApacheWithOidDB")

    //Combine Enriched Rtx Logs with Enriched Apache Logs
    val EnrichedLogsWithDuplicateColumn = ApacheWithOidDB.join(RtxWithOidDB, ApacheWithOidDB("Time")===RtxWithOidDB("TimeRtxLog")
      && ApacheWithOidDB("SessionId")===RtxWithOidDB("sessionIdRtx")
      && ApacheWithOidDB("RequestedPage")===RtxWithOidDB("PageId")
      /*&& ApacheWithOidDB("targetUnit")===RtxWithOidDB("UnitId")*/ ,"left_outer").orderBy(asc("Time"))


    //Rinominate some Column
    val FinalEnrichedLogs=EnrichedLogsWithDuplicateColumn.drop("PageId").drop("unit").drop("sessionIdRtx").drop("TimeRtxLog").drop("EntityId").drop("AttributeId").dropDuplicates().
      withColumnRenamed("SourcePage","SourcePageId").
      withColumnRenamed("LinkClicked","ClickedLinkId").
      withColumnRenamed("RequestedPage","RequestedPageId").
      withColumnRenamed("OidAttribute","ClickedAttributeId").
      withColumnRenamed("OidValue","ClickedOid").
      withColumnRenamed("LinkName","ClickedLinkName").
      withColumnRenamed("EntityClicked","ClickedEntityName").
      withColumnRenamed("ClickedOidDBvalue","ClickedOidValue").
      withColumnRenamed("page","DisplayedPageId").
      withColumnRenamed("Entity","DisplayedEntityId").
      withColumnRenamed("UnitName","DisplayedUnitName").
      withColumnRenamed("EntityName","DisplayedEntityName").
      withColumnRenamed("DisplayedAttribute","DisplayedAttributeId").
      withColumnRenamed("AttributeName","DisplayedAttributeName").
      withColumnRenamed("oidwithDBvalue","DisplayedOid").
      withColumnRenamed("tableColumnwithDBvalue","DisplayedTypeOid").
      withColumnRenamed("oidDBvalue","DisplayedOidValue")//.cache()


    //FinalEnrichedLogs.registerTempTable("EnrichedLogs")

    FinalEnrichedLogs.write.mode("overwrite").parquet(basePath+"data/FinalEnrichedLogs.parquet")

    //Save EnrichedLog as Hive Table
    //FinalEnrichedLogs.write.mode("append").saveAsTable("EnrichedLogs")

    //perform Analysis On Enriched Logs
  //  LogAnalysis.performAnalysis(sc)
  }

}
