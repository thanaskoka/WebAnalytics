package com.webanalytics.analysis

import java.text.{DateFormat, SimpleDateFormat}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.webanalytics.config.DataPreparation
import com.webanalytics.helper.Utilities
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Thanas koka on 04/03/2017.
  */
object LogAnalysis extends DataPreparation{
  var count=1

  def main(args: Array[String]): Unit = {
  //  val conf= new SparkConf().setAppName("WebAnalytics").setMaster("local")

//    val sc = new HiveContext(conf)

    val sc = new SparkContext()
    val sqlContext = new HiveContext(sc)

    def mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    sqlContext.udf.register("sliceString", sliceString)

    var interval=0

    while(true)
    {
      println("Starting analysis number "+count+" \n\n")

      loadConfiguration(sc,sqlContext,args)
     for(i <- 0 to IntervalAnalysis.length - 1){
      performAnalysis(sc,sqlContext,IntervalAnalysis(i))

    }
      count=count+1
      println("\t Starting Complete History analysis ")

      performAnalysis(sc,sqlContext,0)
      println("\t Terminated Complete History analysis \n\n ")

    }

  }


  def performAnalysis(sc:SparkContext,sqlContext: SQLContext,interval :Integer): Unit = {

    if (interval != 0) {
    val timeTreshold = System.currentTimeMillis() - (interval * 60000)
   /* val readEnrichedLogFromCsv=sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").load("FinalEnrichedLogs.csv")
    readEnrichedLogFromCsv.write.mode("overwrite").parquet("FinalEnrichedLogs.parquet")
*/
    val FinalEnrichedLogs = sqlContext.read.parquet(basePath + "data/FinalEnrichedLogs.parquet")
    val FilterdLogs = FinalEnrichedLogs.filter(FinalEnrichedLogs("TimestampIngestion") >= timeTreshold).cache()
    FilterdLogs.registerTempTable("EnrichedLogs")
  }
  else{/*
      val readEnrichedLogFromCsv=sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").load("FinalEnrichedLogs.csv")
      readEnrichedLogFromCsv.write.mode("overwrite").parquet("FinalEnrichedLogs.parquet")
*/
      val FinalEnrichedLogs = sqlContext.read.parquet(basePath + "data/FinalEnrichedLogs.parquet").cache()
      FinalEnrichedLogs.registerTempTable("EnrichedLogs")
    }

    BounceRate(sqlContext)
    EntranceRate(sqlContext)
    AverageVisitsPerPage(sqlContext)
    AverageResidenceTime(sqlContext)
    OutputLink(sqlContext)
    InputLink(sqlContext)
    top10DisplayedViewComponet(sqlContext)
    top10ClickedLink(sqlContext)


    CombineOverallAnalysis(sc,sqlContext,interval)

    }


  def BounceRate(sqlContext: SQLContext):Unit={
    val ExitPageTab =  sqlContext.sql("select last(RequestedPageId) as ExitPageId,last(RequestedPageName) as ExitPageName from EnrichedLogs group by SessionId ")
    ExitPageTab.registerTempTable("ExitPageTab")

    val TabSize=ExitPageTab.count()

    val BounceRateDataView= sqlContext.sql("select  ExitPageId  as UnitId,count(ExitPageId)/ "+TabSize.toString()+ " as BounceRate from ExitPageTab group by ExitPageId,ExitPageName ")//.cache()

   // BounceRateDataView.write.mode("overwrite").parquet(OutputPath+"/BounceRateDataView.parquet")
    //val BounceRateDataView =sqlContext.read.parquet(OutputPath+"/BounceRateDataView.parquet").cache()
    BounceRateDataView.registerTempTable("BounceRateDataView")
    //BounceRateDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/BounceRateDataView.csv")
  }

  def EntranceRate(sqlContext: SQLContext):Unit={
    val EntryPageTab =  sqlContext.sql("select first(RequestedPageId) as EntryPageId,first(RequestedPageName) as EntryPageName from EnrichedLogs group by SessionId ")
    EntryPageTab.registerTempTable("EntryPageTab")

    val TabSize=EntryPageTab.count()

    val EntranceRateDataView= sqlContext.sql("select EntryPageId as UnitId,count(EntryPageId)/ "+TabSize.toString()+ " as EntranceRate from EntryPageTab group by EntryPageId,EntryPageName ")//.cache()

   // EntranceRateDataView.write.mode("overwrite").parquet(OutputPath+"/EntranceRateDataView.parquet")
    //val EntranceRateDataView =sqlContext.read.parquet(OutputPath+"/EntranceRateDataView.parquet").cache()
    EntranceRateDataView.registerTempTable("EntranceRateDataView")
    //EntranceRateDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/EntranceRateDataView.csv")
  }

  def AverageVisitsPerPage(sqlContext: SQLContext):Unit= {
    val AverageVisitTab =  sqlContext.sql("select RequestedPageId,first(RequestedPageName) as RequestedPageName, SessionId,1 as Occurence from EnrichedLogs group by Time,RequestedPageId,SessionId ")
    AverageVisitTab.registerTempTable("AverageVisitTab")

    val NumberOfSessions= sqlContext.sql("select  distinct(SessionId) as NumberOfSessions from EnrichedLogs ").count()


    val AverageVisitsDataView= sqlContext.sql("select RequestedPageId as UnitId,sum(Occurence)/ "+NumberOfSessions+" as AverageVisitsPerPagePerSession from AverageVisitTab group by RequestedPageId,RequestedPageName ")//.cache()

    //AverageVisitsDataView.write.mode("overwrite").parquet(OutputPath+"/AverageVisitsDataView.parquet")
    //val AverageVisitsDataView =sqlContext.read.parquet(OutputPath+"/AverageVisitsDataView.parquet").cache()
    AverageVisitsDataView.registerTempTable("AverageVisitsDataView")
    //AverageVisitsDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/AverageVisitsDataView.csv")

  }

  def AverageResidenceTime(sqlContext: SQLContext):Unit= {
    val PageFlowNavigation=sqlContext.sql("SELECT Time,RequestedPageId, first(RequestedPageName) as RequestedPageName,SessionId,SourcePageId"
      +" FROM EnrichedLogs  GROUP BY Time,RequestedPageId,SessionId,SourcePageId   order by Time  ")//.cache()
    PageFlowNavigation.registerTempTable("PageFlowNavigation")

    val NextNavigation=sqlContext.sql("Select p1.Time,p1.RequestedPageId,p1.RequestedPageName,p1.SessionId,first(p2.Time) as TimeNextPage,first(p2.RequestedPageId) "
      +" as NextPageId from PageFlowNavigation as p1 inner join PageFlowNavigation as p2  "
      +" on p1.SessionId=p2.SessionId and (p2.RequestedPageId != p1.RequestedPageId and p2.Time>p1.Time and "
      +" (   unix_timestamp(p2.Time)<unix_timestamp(p1.Time)+3600 )) group By p1.Time, "
      +" p1.RequestedPageId,p1.RequestedPageName,p1.SessionId order by Time asc")//.cache()

    NextNavigation.registerTempTable("NextNavigation")

    val PageFlowNextNavigation=sqlContext.sql("Select first(Time) as Time,RequestedPageId,RequestedPageName,TimeNextPage from NextNavigation group by RequestedPageId,RequestedPageName,TimeNextPage,NextPageId ")
    PageFlowNextNavigation.registerTempTable("PageFlowNextNavigation")

    val DeriveResidenceTime=sqlContext.sql("Select RequestedPageId as UnitId,(unix_timestamp(TimeNextPage)- unix_timestamp(Time)) as ResidenceTime from PageFlowNextNavigation ")
    DeriveResidenceTime.registerTempTable("DeriveResidenceTime")

    val AverageResidenceTimeDataView=sqlContext.sql("Select UnitId,avg( ResidenceTime) as ResidenceTime  from DeriveResidenceTime group by UnitId ")//.cache()

   // AverageResidenceTimeDataView.write.mode("overwrite").parquet(OutputPath+"/AverageResidenceTimeDataView.parquet")
    //val AverageResidenceTimeDataView =sqlContext.read.parquet(OutputPath+"/AverageResidenceTimeDataView.parquet").cache()
    AverageResidenceTimeDataView.registerTempTable("AverageResidenceTimeDataView")
    //AverageResidenceTimeDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/AverageResidenceTimeDataView.csv")
  }

  def OutputLink(sqlContext: SQLContext):Unit= {
    val LinkFlow=sqlContext.sql(" select SourcePageId,RequestedPageId,SourcePageName ,ClickedLinkId,ClickedLinkName,1 as Occurence from EnrichedLogs where "
      +" (ClickedLinkId is not null and ClickedLinkId !='null' ) group by Time,SourcePageId,RequestedPageId,SourcePageName ,ClickedLinkId,ClickedLinkName having "
      +" SourcePageId!=RequestedPageId ")
    LinkFlow.registerTempTable("LinkFlow")

    val CountLinkOccurance=sqlContext.sql(" select SourcePageId,RequestedPageId,ClickedLinkId,ClickedLinkName,sum(Occurence) as Sum from LinkFlow  "
      +" group by SourcePageId,RequestedPageId,SourcePageName ,ClickedLinkId,ClickedLinkName ")
    CountLinkOccurance.registerTempTable("CountLinkOccurance")

    val LinkOutputPercentageDataView=sqlContext.sql(" select t1.ClickedLinkId as UnitId,(t1.Sum/Tot) as LinkOut from CountLinkOccurance t1 join (select SourcePageId,sum(Sum) as Tot  from CountLinkOccurance group by SourcePageId) t2  on t1.SourcePageId=t2.SourcePageId ")//.cache()
    LinkOutputPercentageDataView.registerTempTable("LinkOutputPercentageDataView")



    //LinkOutputPercentageDataView.write.mode("overwrite").parquet(OutputPath+"/LinkOutputPercentageDataView.parquet")
    //val LinkOutputPercentageDataView =sqlContext.read.parquet(OutputPath+"/LinkOutputPercentageDataView.parquet").cache()
    LinkOutputPercentageDataView.registerTempTable("LinkOutputPercentageDataView")
    //LinkOutputPercentageDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/LinkOutputPercentageDataView.csv")
  }

  def InputLink(sqlContext: SQLContext):Unit= {
    val LinkFlow=sqlContext.sql(" select SourcePageId,RequestedPageId,RequestedPageName ,ClickedLinkId,ClickedLinkName,1 as Occurence from EnrichedLogs where "
      +" (ClickedLinkId is not null and ClickedLinkId !='null' ) group by Time,SourcePageId,RequestedPageId,RequestedPageName ,ClickedLinkId,ClickedLinkName having "
      +" SourcePageId!=RequestedPageId ")
    LinkFlow.registerTempTable("LinkFlow")

    val CountLinkOccurance=sqlContext.sql(" select SourcePageId,RequestedPageId,ClickedLinkId,ClickedLinkName,sum(Occurence) as Sum from LinkFlow  "
      +" group by SourcePageId,RequestedPageId,RequestedPageId ,ClickedLinkId,ClickedLinkName ")
    CountLinkOccurance.registerTempTable("CountLinkOccurance")

    val LinkInputPercentageDataView=sqlContext.sql(" select t1.ClickedLinkId as UnitId,(t1.Sum/Tot) as LinkIn from CountLinkOccurance t1 join (select RequestedPageId,sum(Sum) as Tot  from CountLinkOccurance group by RequestedPageId) t2  on t1.RequestedPageId=t2.RequestedPageId ")//.cache()

    LinkInputPercentageDataView.registerTempTable("LinkInputPercentageDataView")

    //LinkInputPercentageDataView.write.mode("overwrite").parquet(OutputPath+"/LinkInputPercentageDataView.parquet")
    //val LinkInputPercentageDataView =sqlContext.read.parquet(OutputPath+"/LinkInputPercentageDataView.parquet").cache()
    //LinkInputPercentageDataView.registerTempTable("LinkInputPercentageDataView")
    //LinkInputPercentageDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/LinkInputPercentageDataView.csv")
  }

def top10DisplayedViewComponet(sqlContext: SQLContext):Unit={

  val DisplayedAttribute=sqlContext.sql("select UnitId,DisplayedUnitName,DisplayedAttributeName,DisplayedOidValue,1 as Occurence from EnrichedLogs where "
    +" (DisplayedOidValue is not null  and DisplayedOidValue!='NULL') and (DisplayedAttributeName ='name' or DisplayedAttributeName ='title' or "
    +" DisplayedAttributeName ='category' )  group By Time,UnitId,DisplayedUnitName,DisplayedOidValue,DisplayedAttributeName ")
  DisplayedAttribute.registerTempTable("DisplayedAttribute")

  val CountDisplayedOccurance=sqlContext.sql(" select UnitId,DisplayedUnitName,DisplayedOidValue,cast( sum(Occurence) as int) as Sum from DisplayedAttribute  "
    +" group by UnitId,DisplayedUnitName,DisplayedOidValue order by UnitId,Sum desc ")
  CountDisplayedOccurance.registerTempTable("CountDisplayedOccurance")

  val concatValueToAttribute=sqlContext.sql(" select UnitId,DisplayedUnitName,DisplayedOidValue as Top10DisplayedInstances  from CountDisplayedOccurance")
  concatValueToAttribute.registerTempTable("concatValueToAttribute")


  val Top10DisplayedInstancesDataView=sqlContext.sql(" select UnitId as UnitId,sliceString(collect_list(Top10DisplayedInstances),10) as Top10DisplayedInstances from concatValueToAttribute  group by UnitId,DisplayedUnitName ")//.cache()
  Top10DisplayedInstancesDataView.registerTempTable("Top10DisplayedInstancesDataView")

  //Top10DisplayedInstancesDataView.write.mode("overwrite").parquet(OutputPath+"/Top10DisplayedInstancesDataView.parquet")
  //val Top10DisplayedInstancesDataView =sqlContext.read.parquet(OutputPath+"/Top10DisplayedInstancesDataView.parquet").cache()
  //Top10DisplayedInstancesDataView.registerTempTable("Top10DisplayedInstancesDataView")
 // Top10DisplayedInstancesDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/Top10DisplayedInstancesDataView.csv")
}

 def top10ClickedLink(sqlContext: SQLContext) :Unit={


   val ClickedAttribute=sqlContext.sql("select ClickedLinkId,ClickedLinkName,ClickedTypeOid,ClickedOidValue,1 as Occurence from EnrichedLogs where "
     +" (ClickedOidValue is not null and ClickedLinkName is not null  and ClickedOidValue!='NULL') and (ClickedTypeOid ='NAME' or ClickedTypeOid ='TITLE' or "
     +" ClickedTypeOid ='CATEGORY' )  group By Time,ClickedLinkId,ClickedLinkName,ClickedOidValue,ClickedTypeOid ")
   ClickedAttribute.registerTempTable("ClickedAttribute")

   val CountClickedOccurance=sqlContext.sql(" select ClickedLinkId,ClickedLinkName,ClickedOidValue,cast( sum(Occurence) as int) as Sum from ClickedAttribute  "
     +" group by ClickedLinkId,ClickedLinkName,ClickedOidValue order by ClickedLinkId,Sum desc ")
   CountClickedOccurance.registerTempTable("CountClickedOccurance")


   val concatValueToClickedAttribute=sqlContext.sql(" select ClickedLinkId,ClickedLinkName,ClickedOidValue as Top10ClickedInstances  from CountClickedOccurance")
   concatValueToClickedAttribute.registerTempTable("concatValueToClickedAttribute")

   val Top10ClickedInstancesDataView=sqlContext.sql(" select ClickedLinkId as UnitId,sliceString(collect_list(Top10ClickedInstances),10) as Top10ClickedInstances from concatValueToClickedAttribute  group by ClickedLinkId,ClickedLinkName ")//.cache()

   //Top10ClickedInstancesDataView.write.mode("overwrite").parquet(OutputPath+"/Top10ClickedInstancesDataView.parquet")
   //val Top10ClickedInstancesDataView =sqlContext.read.parquet(OutputPath+"/Top10ClickedInstancesDataView.parquet").cache()
   Top10ClickedInstancesDataView.registerTempTable("Top10ClickedInstancesDataView")
   //Top10ClickedInstancesDataView.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+"/Top10ClickedInstancesDataView.csv")
 }

  def CombineOverallAnalysis(sc:SparkContext,sqlContext: SQLContext,interval: Integer):Unit={

    if(interval!=0){
    println("\t Starting Analyze "+interval+" Minutes Interval")
    }

    val actualtimemillis=System.currentTimeMillis()
    val formatter :DateFormat = new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss")
    val timestampAnalysis = formatter.format(actualtimemillis)
    val AnalysisType="/Analysis"+interval+"Minutes"
    val statisticPath=AnalysisType+"/CombinedAnalysis"+"_"+timestampAnalysis

    var CombinedAnalysis=sqlContext.sql("Select t1.*,t2.AverageVisitsPerPagePerSession from AverageResidenceTimeDataView t1  full  outer join AverageVisitsDataView t2 on t1.UnitId=t2.UnitId  ")
    CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis=sqlContext.sql("Select t1.*,t2.BounceRate from CombinedAnalysis t1  full outer join BounceRateDataView t2 on t1.UnitId=t2.UnitId  ")
    CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis=sqlContext.sql("Select t1.*,t2.EntranceRate from CombinedAnalysis t1  full outer join EntranceRateDataView t2 on t1.UnitId=t2.UnitId  ")
    CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis=sqlContext.sql("Select  if(t1.UnitId IS NULL, t2.UnitId, t1.UnitId) AS UnitId,t1.ResidenceTime,t1.AverageVisitsPerPagePerSession,t1.BounceRate,t1.EntranceRate,t2.LinkOut from CombinedAnalysis t1  full outer join LinkOutputPercentageDataView t2 on t1.UnitId=t2.UnitId  ")
    CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis=sqlContext.sql("Select  if(t1.UnitId IS NULL, t2.UnitId, t1.UnitId) AS UnitId,t1.ResidenceTime,t1.AverageVisitsPerPagePerSession,t1.BounceRate,t1.EntranceRate,t1.LinkOut,t2.LinkIn from CombinedAnalysis t1  full outer join LinkInputPercentageDataView t2 on t1.UnitId=t2.UnitId  ")
    CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis=sqlContext.sql("Select  if(t1.UnitId IS NULL, t2.UnitId, t1.UnitId) AS UnitId,t1.ResidenceTime,t1.AverageVisitsPerPagePerSession,t1.BounceRate,t1.EntranceRate,t1.LinkOut,t1.LinkIn,t2.Top10ClickedInstances from CombinedAnalysis t1  full outer join Top10ClickedInstancesDataView t2 on t1.UnitId=t2.UnitId  ")
    CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis=sqlContext.sql("Select  if(t1.UnitId IS NULL, t2.UnitId, t1.UnitId) AS UnitId,t1.ResidenceTime,t1.AverageVisitsPerPagePerSession,t1.BounceRate,t1.EntranceRate,t1.LinkOut,t1.LinkIn,t1.Top10ClickedInstances,t2.Top10DisplayedInstances from CombinedAnalysis t1  full outer join Top10DisplayedInstancesDataView t2 on t1.UnitId=t2.UnitId  ")//.cache()
    //CombinedAnalysis.registerTempTable("CombinedAnalysis")

    CombinedAnalysis.cache()
    //val CombinedAnalysis =sqlContext.read.parquet(OutputPath+AnalysisType+"/CombinedAnalysis.parquet").cache()
    CombinedAnalysis.registerTempTable("CombinedAnalysis")
    //CombinedAnalysis.repartition(1).write.mode("overwrite").parquet(OutputPath+statisticPath+".parquet")
    //CombinedAnalysis.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "true").save(OutputPath+statisticPath+".csv")
    //CombinedAnalysis.repartition(1).write.mode("overwrite").format("org.apache.spark.sql.json").save(OutputPath+statisticPath+".json")
    import sqlContext.implicits._
    //Build Analytics JSON OBJECT
    val TokenizedString=Utilities.buildJsonObject(sqlContext)
    val JsonDF=sc.parallelize(Seq(TokenizedString)).toDF

    //write output to one file
    JsonDF.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").save(OutputPath+statisticPath+".json")

   // JsonDF.coalesce(1, true).mode("overwrite").saveAsTextFile(OutputPath+statisticPath+".json")
    println("\t Terminated  "+interval+" Minutes Interval Analysis \n")

  }
    //Helper Functions to perform TOP-k Analysis : Take just the top-k over the entire List
  def sliceString = (list : Seq[String], top : Int) => {
    val   ListSize=list.length
    if(ListSize<top){
      list.slice(0,ListSize)}
    else{list.slice(0,top)}
  }
}


