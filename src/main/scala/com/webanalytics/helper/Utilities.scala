package com.webanalytics.helper

import java.sql.Timestamp
import com.webanalytics.config.DataPreparation
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
/**
  * Created by Thanas koka on 01/03/2017.
  */
object Utilities extends DataPreparation {

  def parseWebModel_with4Attribute(line: String,sqlContext: SQLContext): DataFrame = {

    try{
      val df=sqlContext.read
        .format("xml")
        .option("rowTag", line)
        .load(WebModelpath)
        .select("@id","@name","@entity","@displayAttributes")
        .toDF(Seq("UnitId", "UnitName","Entity",  "DisplayAttributes"):_*)

      return df
    }
    catch{
      case x: org.apache.spark.sql.AnalysisException =>  return  null
    }
  }

  //Helper Functions to parse Web Model
  def parseWebModel_with3Attribute(line: String,sqlContext: SQLContext): DataFrame = {

    try{
      val df=sqlContext.read
        .format("xml")
        .option("rowTag", line)
        .load(WebModelpath)
        .select("@id","@name","@entity")
        .toDF(Seq("UnitId", "UnitName","Entity"):_*)
        .withColumn("DisplayedAttribute",lit(null))

      return df
    }
    catch{
      case x: org.apache.spark.sql.AnalysisException =>  return  null
    }
  }

  def parseWebModel_with2Attribute(line: String,sqlContext: SQLContext): DataFrame = {

    try{
      val df=sqlContext.read
        .format("xml")
        .option("rowTag", line)
        .load(WebModelpath)
        .select("@id","@name")
        .toDF(Seq("UnitId", "UnitName"):_*)
        .withColumn("Entity",lit(null))
        .withColumn("DisplayedAttribute",lit(null))

      return df
    }
    catch{
      case x: org.apache.spark.sql.AnalysisException =>  return  null
    }
  }



  def parseLink(sqlContext: SQLContext) :DataFrame={
    val PartialLink=sqlContext.read
      .format("xml")
      .option("rowTag", "Link")
      .load(WebModelpath)
      .select("@id","@name","@to")
      .toDF(Seq("LinkId", "LinkName","targetUnit"):_*)

    val LinkRdd=PartialLink.map ( x => Row( x.getAs[String]("LinkId").split("#").last ,x.getAs[String]("LinkName"),x.getAs[String]("targetUnit").split("#").last))
    val ParsedFlowLink = sqlContext.createDataFrame(LinkRdd, PartialLink.schema)

    val OkLink=sqlContext.read
      .format("xml")
      .option("rowTag", "OKLink")
      .load(WebModelpath)
      .select("@id","@name","@to")
      .toDF(Seq("LinkId", "LinkName","targetUnit"):_*)

    val OkRdd=OkLink.map ( x => Row( x.getAs[String]("LinkId").split("#").last ,x.getAs[String]("LinkName"),x.getAs[String]("targetUnit").split("#").last))

    val OkParsedLink = sqlContext.createDataFrame(OkRdd, OkLink.schema)


    val KoLink=sqlContext.read
      .format("xml")
      .option("rowTag", "KOLink")
      .load(WebModelpath)
      .select("@id","@name","@to")
      .toDF(Seq("LinkId", "LinkName","targetUnit"):_*)

    val KoRdd=KoLink.map ( x => Row( x.getAs[String]("LinkId").split("#").last ,x.getAs[String]("LinkName"),x.getAs[String]("targetUnit").split("#").last))
    val KoParsedLink = sqlContext.createDataFrame(KoRdd, KoLink.schema)

    val ParsedLink=ParsedFlowLink.unionAll(OkParsedLink).unionAll(KoParsedLink)
    ParsedLink
  }

  def parseViewComponent(sqlContext: SQLContext):DataFrame={
    val PartialWM4Att=Units4Att.map(line=>Utilities.parseWebModel_with4Attribute(line,sqlContext))
      .filter( x => x!=null )
      .reduce((x,y)=>x.unionAll(y))

    val PartialWM3Att=Units3Att.map(line=>Utilities.parseWebModel_with3Attribute(line,sqlContext))
      .filter( x => x!=null )
      .reduce((x,y)=>x.unionAll(y))
    val PartialWM2Att=Units2Att.map(line=>Utilities.parseWebModel_with2Attribute(line,sqlContext))
      .filter( x => x!=null )
      .reduce((x,y)=>x.unionAll(y))

    val WebModel2and3Att=PartialWM2Att.unionAll(PartialWM3Att)
    val WebModelSchema =  StructType(Array(
      StructField("UnitId", StringType, nullable = true),
      StructField("UnitName", StringType, nullable = true),
      StructField("Entity", StringType ,nullable = true),
      StructField("DisplayAttributes", ArrayType(StringType, true) ,nullable=true)))

    val WebModelRdd4Att=PartialWM4Att.map( x => Row( x.getAs[String]("UnitId").split("#").last ,x.getAs[String]("UnitName"),x.getAs[String]("Entity"),x.getAs[String]("DisplayAttributes").split(" ").map( x => x.split("#").last) ))

    val WebModelRdd2and3Att=WebModel2and3Att.map ( x => Row( x.getAs[String]("UnitId").split("#").last ,x.getAs[String]("UnitName"),x.getAs[String]("Entity"),x.getAs[String]("DisplayedAttribute") ))

    val WebModel4AttExploded = sqlContext.createDataFrame(WebModelRdd4Att, WebModelSchema)
      .explode("DisplayAttributes","DisplayedAttribute")({DisplayAttributes: Seq[String]=> DisplayAttributes })
      .drop("DisplayAttributes")


    val WebModel2And3AttParsed = sqlContext.createDataFrame(WebModelRdd2and3Att, WebModel2and3Att.schema)
    val ParsedWebModel=WebModel4AttExploded.unionAll(WebModel2And3AttParsed)
    ParsedWebModel.registerTempTable("WebModel")
    ParsedWebModel
  }

  def parsePagesWebModel(sqlContext: SQLContext):DataFrame={



    val PartialPageWebModel=Pages.map(line=>parsePages(line,sqlContext))
      .filter( x => x!=null )

    val PageWebModel=PartialPageWebModel(0).unionAll(PartialPageWebModel(1))

    val PageWebModelRdd=PageWebModel.map ( x => Row( x.getAs[String]("PageId").split("#").last ,x.getAs[String]("PageName")))

    val ParsedPageWebModel = sqlContext.createDataFrame(PageWebModelRdd, PageWebModel.schema)

    ParsedPageWebModel
  }

  def parseDataModel(sqlContext: SQLContext):DataFrame={

    var DataModel = sqlContext.read
      .format("xml")
      .option("rowTag", "Entity")
      .load(DataModelpath)
      .select("@id","@name","@table","Attribute.@id","Attribute.@name","Attribute.@column")
      .toDF(Seq("EntityId","EntityName","TableDB","AttributeId","AttributeName","AttributeDatabaseColumn"): _*)


    val unify3columns = udf[Seq[(String, String, String)], Seq[String], Seq[String], Seq[String]]{
      case (a, b, c) => (a,b, c).zipped.toSeq
    }

    val tempDataModel = DataModel.select(DataModel("EntityId"),DataModel("EntityName"),DataModel("TableDB"),unify3columns(DataModel("AttributeId"),DataModel("AttributeName"),DataModel("AttributeDatabaseColumn")).alias("UnifiedColumns"))

    val explodedDataModel = tempDataModel.select(tempDataModel("EntityId"),tempDataModel("TableDB"),tempDataModel("EntityName"),explode(tempDataModel("UnifiedColumns")).alias("UnifiedColumns"))

    DataModel=explodedDataModel.withColumn("AttributeId", explodedDataModel("UnifiedColumns").getItem("_1"))
      .withColumn("AttributeName", explodedDataModel("UnifiedColumns").getItem("_2"))
      .withColumn("AttributeDatabaseColumn", explodedDataModel("UnifiedColumns").getItem("_3"))
      .drop("UnifiedColumns")


    val DataModelRdd=DataModel.map ( x => Row( x.getAs[String]("EntityId") ,x.getAs[String]("TableDB"),x.getAs[String]("EntityName"),x.getAs[String]("AttributeId").split("#").last,x.getAs[String]("AttributeName"),x.getAs[String]("AttributeDatabaseColumn")) )
      .filter(x=> x(5)!= null)//aggiunto 23 novembre primann c'era


    val ParsedDataModel = sqlContext.createDataFrame(DataModelRdd, DataModel.schema)
    ParsedDataModel
  }


    def parseDbIstance(path: String,sqlContext: SQLContext): DataFrame = {
      val df2 :DataFrame =null

      try{
        val df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "false") // Automatically infer data types
          .option("delimiter", ";")
          .load(PersistentDatapath+path+".csv")
          .withColumn("TableName", lit(path))

        df.registerTempTable(path)

        return df
      }
      catch{
        case x: org.apache.spark.sql.AnalysisException =>  return df2
      }
    }


  case class ApacheLog(
                        IpAdress: String,
                        Time: Timestamp,
                        RequestedPage: String,
                        OidAttribute: String,
                        OidValue: Int,
                        LinkClicked: String,
                        SourcePage: String,
                        SessionId: String,
                        TimestampIngestion: Long
                      ){
    override def toString =" "+ApacheDateFormat.format(Time)+  s" $IpAdress  $SourcePage  $LinkClicked  $RequestedPage     $OidAttribute       $OidValue   $SessionId    "           }

  val ApachePatt1="""(?:(\S+).*\[(\d+)\/(\w+)\/(\d+):(\d+):(\d+):(\d+).*\].*\/(.*).do\?.*?(?:(?=.*sp=(\w+))(?=.*link=(\w+))(?:.*?(att\d+)=(\d+))).*\" (?:\d+) (?:\d+) (\S+))""".r
  val ApachePatt2="""(?:(\S+).*\[(\d+)\/(\w+)\/(\d+):(\d+):(\d+):(\d+).*\].*\/(.*).do\?.*?(?:(?=.*link=(\w+))(?:.*?(att\d+)=(\d+))).*\" (?:\d+) (?:\d+) (\S+))""".r
  val ApachePatt3="""(?:(\S+).*\[(\d+)\/(\w+)\/(\d+):(\d+):(\d+):(\d+).*\].*\/(m?page.*).do) HTTP.*\" (?:\d+) (?:\d+) (\S+)""".r
  val ApachePatt4="""(?:(\S+).*\[(\d+)\/(\w+)\/(\d+):(\d+):(\d+):(\d+).*\].*\/(.*).do).*(?=.*sp=(\w+)).+\" (?:\d+) (?:\d+) (\S+)""".r
  val ApachePatt5="""(?:(\S+).*\[(\d+)\/(\w+)\/(\d+):(\d+):(\d+):(\d+).*\].*\/(.*).do;jsessionid=(\S+) .*)""".r


  val pattern=(ApachePatt1+"|"+ApachePatt2+"|"+ApachePatt3+"|"+ApachePatt4+"|"+ApachePatt5).r

  def getTimestamp( day: String,month: String,year: String,hour: String,minute: String,seconds: String): Timestamp={
    return new java.sql.Timestamp(ApacheDateFormat.parse(day+"/"+month+"/"+year+":"+hour+":"+minute+":"+seconds).getTime)
  }

  def getCurrentTimestamp():Long={
    val currentTs: Long = System.currentTimeMillis()
    currentTs
  }

  def parseApacheLine(line: String): ApacheLog = {

    pattern.findFirstIn(line) match {
      case Some(ApachePatt1(ip,day,month,year,hour,minute,seconds,requestedPage,sourcePage,linkClicked,oidAttribute,oidValue,sessionId)) =>ApacheLog(ip,getTimestamp(day,month,year,hour,minute,seconds),requestedPage,oidAttribute,oidValue.toInt,linkClicked,sourcePage,sessionId,getCurrentTimestamp)

      case Some(ApachePatt2(ip,day,month,year,hour,minute,seconds,requestedPage,linkClicked,oidAttribute,oidValue,sessionId)) =>ApacheLog(ip,getTimestamp(day,month,year,hour,minute,seconds),requestedPage,oidAttribute,oidValue.toInt,linkClicked,requestedPage,sessionId,getCurrentTimestamp)

      case Some(ApachePatt3(ip,day,month,year,hour,minute,seconds,requestedPage,sessionId)) =>ApacheLog(ip,getTimestamp(day,month,year,hour,minute,seconds),requestedPage,null,0,null,null,sessionId,getCurrentTimestamp)

      case Some(ApachePatt4(ip,day,month,year,hour,minute,seconds,requestedPage,sourcePage,sessionId)) =>ApacheLog(ip,getTimestamp(day,month,year,hour,minute,seconds),requestedPage,null,0,null,sourcePage,sessionId,getCurrentTimestamp)

      case Some(ApachePatt5(ip,day,month,year,hour,minute,seconds,requestedPage,sessionId)) =>ApacheLog(ip,getTimestamp(day,month,year,hour,minute,seconds),requestedPage,null,0,null,null,sessionId,getCurrentTimestamp)

      case _=> null

    }
  }

  def returnAtt(attline: String): String={
    try{
      return attline.split("\\.").last
    }catch{
      case nullpointerexception: java.lang.NullPointerException =>  return null

    }

  }



  def parseApacheLogs(sc: SparkContext,sqlContext: SQLContext,ParsedDataModel:DataFrame,ParsedPageWebModel:DataFrame) :DataFrame={
    import sqlContext.implicits._
    val ApacheLogDataSet = sc.textFile(ApacheLogPath)//.cache()

    val  ApacheLogParsed=ApacheLogDataSet.map(parseApacheLine).filter(m=> m!=null && m.TimestampIngestion>= oldTimestamp).toDF()//.cache()
    saveLastTimestampIngestion(sc,sqlContext)

    val PartialwithReqPageApacheLog=ApacheLogParsed.as('a).join(ParsedDataModel.as('b), $"a.OidAttribute" === $"b.AttributeId","left_outer").select($"a.IpAdress", $"a.Time",$"a.RequestedPage", $"a.OidAttribute",$"b.EntityId".as('EntityIdClicked), $"b.EntityName".as('EntityClicked),$"a.OidValue",$"a.LinkClicked",$"a.SourcePage",$"a.SessionId").join(ParsedPageWebModel,$"RequestedPage"===$"PageId" ).drop("PageId").withColumnRenamed("PageName","RequestedPageName")

    val PartialApacheLog=PartialwithReqPageApacheLog.as('x).join(ParsedPageWebModel.as('y),$"x.SourcePage"===$"y.PageId","left_outer" ).drop("PageId").withColumnRenamed("PageName","SourcePageName")
    PartialApacheLog
  }



  case class RtxLog(
                     Time: Timestamp,
                     LogLevel: String,
                     host: String,
                     javaclass: String,
                     sessionId: String,
                     miuId: String,
                     page: String,
                     unit: String,
                     trace: String,
                     message: String,
                     oids: Array[Int],
                     TimestampIngestion:Long){
    override def toString =" "+DateFormat.format(Time)+  s"    $LogLevel   $host    $javaclass   $sessionId   $miuId     $page     $unit  $message   $trace    "
  }

  val patt1="""(?:(\w+) (\w+) (\d+) (\d+):(\d+):(\S+)  (\w+) \[([^]]+)\]  \((.*)\) - \[(\w+)\](?:\[(\w+)\])?\[(\w+)\]\[(\w+)\]\[\w+\]\[([^]]+)\](.*\[(.+)\]}))""".r
  val patt2="""(?:(\w+) (\w+) (\d+) (\d+):(\d+):(\S+)  (\w+) \[([^]]+)\]  \((.*)\) - \[(\w+)\](?:\[(\w+)\])?\[(\w+)\]\[(\w+)\]\[\w+\]\[([^]]+)\](.*\{\}))""".r
  val patt3="""(?:(\w+) (\w+) (\d+) (\d+):(\d+):(\S+)  (\w+) \[([^]]+)\]  \((.*)\) - \[(\w+)\](?:\[(\w+)\])?\[(\w+)\]\[(\w+)\](.*))""".r
  val patt4="""(?:(\w+) (\w+) (\d+) (\d+):(\d+):(\S+)  (\w+) \[([^]]+)\]  \((.*)\) - \[(\w+)\]\[(\w+)\](.*))""".r
  val patt=(patt1+"|"+patt2+"|"+patt3+"|"+patt4).r

  def splitOids(oids:String): Array[Int]={

    val intArr=oids.split(", ")map(x=> x.toInt)
    val x = intArr//.toList
    return x
  }


  def parseLine(line: String): RtxLog = {

    patt.findFirstIn(line) match {
      case Some(patt1(day,month,year,hour,minute,seconds,level,host,javaclass,sessionId,miuId,page,unit,trace,message,oids)) =>RtxLog(getTimestamp(day,month,year,hour,minute,seconds),level,host,javaclass,sessionId,miuId,page,unit,trace,message,splitOids(oids),getCurrentTimestamp)
      case Some(patt2(day,month,year,hour,minute,seconds,level,host,javaclass,sessionId,miuId,page,unit,trace,message)) =>RtxLog(getTimestamp(day,month,year,hour,minute,seconds),level,host,javaclass,sessionId,miuId,page,unit,trace,message,null,getCurrentTimestamp)
      case Some(patt3(day,month,year,hour,minute,seconds,level,host,javaclass,sessionId,miuId,page,unit,message)) =>RtxLog(getTimestamp(day,month,year,hour,minute,seconds),level,host,javaclass,sessionId,miuId,page,unit,null,message,null,getCurrentTimestamp)
      case Some(patt4(day,month,year,hour,minute,seconds,level,host,javaclass,sessionId,page,message)) =>RtxLog(getTimestamp(day,month,year,hour,minute,seconds),level,host,javaclass,sessionId,null,page,null,null,message,null,getCurrentTimestamp)
      case _=> null
    }
  }


  def parseRtxLogs(sc:SparkContext, sqlContext: SQLContext,ParsedPageWebModel:DataFrame):DataFrame={
    import sqlContext.implicits._

    val dataSet = sc.textFile(RtxLogPath)//.cache()

    val ParsedLogRdd=dataSet.map(parseLine).filter(log=> log!=null  && log.TimestampIngestion>= oldTimestamp)//.cache()

    val ParsedLogDf=ParsedLogRdd.toDF()

    val exploded= ParsedLogDf.where($"oids".isNotNull).explode("oids","oid")({oids: Seq[Int]=> oids })

    val ParsedLog=ParsedLogDf.where($"oids".isNull).withColumn("oid",  lit(null)).unionAll(exploded)

    val ParsedLogWithOid=ParsedLog.where($"oid".isNotNull).drop("oids")

    val EnrichedLog=ParsedLogWithOid.join(ParsedPageWebModel,(ParsedLogWithOid("page")===ParsedPageWebModel("PageId")))
    EnrichedLog
  }


  def enrichApacheLogs(sqlContext: SQLContext,PartialApacheLog:DataFrame,parsedCombinedModels:DataFrame,ParsedLink:DataFrame):DataFrame={
    val FinalApacheLog=PartialApacheLog.as('a).join(parsedCombinedModels.as('b), PartialApacheLog("EntityIdClicked")===parsedCombinedModels("EntityId"),"left_outer").select(PartialApacheLog("IpAdress"), PartialApacheLog("Time"),PartialApacheLog("SourcePage"),PartialApacheLog("LinkClicked"),PartialApacheLog("RequestedPage"), PartialApacheLog("OidAttribute"),PartialApacheLog("EntityIdClicked"),PartialApacheLog("EntityClicked"),parsedCombinedModels("TableDB"), parsedCombinedModels("AttributeDatabaseColumn"),PartialApacheLog("OidValue"),PartialApacheLog("SessionId"),PartialApacheLog("SourcePageName"),PartialApacheLog("RequestedPageName"))
    val ApacheLogandLinks= FinalApacheLog.join(ParsedLink,FinalApacheLog("LinkClicked")=== ParsedLink("LinkId"),"left_outer").select(FinalApacheLog("IpAdress"), FinalApacheLog("Time"),FinalApacheLog("SourcePage"),FinalApacheLog("LinkClicked"),FinalApacheLog("RequestedPage"), FinalApacheLog("OidAttribute"),FinalApacheLog("EntityIdClicked"),FinalApacheLog("TableDB"), FinalApacheLog("AttributeDatabaseColumn"),FinalApacheLog("OidValue"),FinalApacheLog("SessionId"),FinalApacheLog("SourcePageName"),ParsedLink("LinkName"),FinalApacheLog("EntityClicked"),FinalApacheLog("RequestedPageName"),ParsedLink("targetUnit"))//.orderBy(asc("Time"))
      ApacheLogandLinks.registerTempTable("ApacheLogandLinks")
    ApacheLogandLinks
  }


  def addOidValueDb(sc:SparkContext,sqlContext: SQLContext):DataFrame={
    import sqlContext.implicits._



    val TupleApacheToBuildQuery=sqlContext.sql(" SELECT OidValue, TableDB , AttributeDatabaseColumn FROM ApacheLogandLinks Where OidValue!=0  GROUP BY  OidValue, TableDB , AttributeDatabaseColumn ")//.cache()

    val TupleRtxToBuildQuery=sqlContext.sql(" SELECT oid, TableDB , AttributeDatabaseColumn FROM EnrichedRtx GROUP BY  oid, TableDB , AttributeDatabaseColumn ")//.cache()

    val TupleToBuildQuery=TupleRtxToBuildQuery.unionAll(TupleApacheToBuildQuery)//.cache()


    val CollectedTuple=TupleToBuildQuery.rdd.collect()
    val BuildOidValueColumn=CollectedTuple.map(x=>(setValueOid(x(0).asInstanceOf[Int],x(1).asInstanceOf[String],x(2).asInstanceOf[String],sqlContext)))

    val OidCol=sc.parallelize(BuildOidValueColumn)


    val OidDB=OidCol.map(
      {
        case Row(oid:Int ,tableName: String,tableColumn: String ,oidDB: String) => (oid ,tableName,tableColumn ,oidDB)
      }).toDF("oidwithDBvalue","tableNamewithDBvalue","tableColumnwithDBvalue","oidDBvalue").cache()//filter("""oidDBvalue is not null and oidDBvalue!="NULL" """).cache()

    OidDB.registerTempTable("OidDB")
    OidDB
  }


  def saveLastTimestampIngestion(sc:SparkContext,sqlContext: SQLContext):Unit={
  import sqlContext.implicits._
  val timestamp=System.currentTimeMillis()
    val x:Seq[Long] =Seq(timestamp)
    val df=sc.parallelize(x).toDF()
    df.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").save(basePath + "oldTimestampIngestion.csv")
  }

  def readLastTimestampIngestion(sqlContext: SQLContext):Long={
    val readLastTimestamp = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ";").load(basePath + "oldTimestampIngestion.csv")
    val lastTimestampIngestion=readLastTimestamp.collect()(0).getString(0).toLong
    lastTimestampIngestion
  }

  def parsePages(line: String,sqlContext: SQLContext): DataFrame = {
    val df2 :DataFrame =null

    try{

      val df=sqlContext.read
        .format("xml")
        .option("rowTag", line)
        .load(WebModelpath)
        .select("@id","@name")
        .toDF(Seq("PageId", "PageName"):_*)

      return df }
    catch{
      case x: org.apache.spark.sql.AnalysisException =>  return df2
    }
  }


  def setValueOid = (oid: Int,TableName: String, TableColumn: String,sqlContext: SQLContext) => {
    val curroid= oid.toString
    val currTableName= TableName
    val currTableColumn= TableColumn

    try{
      val oidDB=sqlContext.sql(s"Select $currTableColumn from $currTableName where OID = $curroid ").first()
      Row(oid,currTableName,currTableColumn,oidDB.getString(0))
    }
    catch{
      case nullExcep: java.lang.NullPointerException =>  Row(oid,currTableName,currTableColumn,"NULL")
      case analysis: org.apache.spark.sql.AnalysisException =>  Row(oid,currTableName,currTableColumn,"NULL")
      case noElem: java.util.NoSuchElementException=>  Row(oid,currTableName,currTableColumn,"NULL")
      case timeToString: java.lang.ClassCastException => Row(oid,currTableName,currTableColumn,"NULL")
      case  _: Throwable => Row(oid,currTableName,currTableColumn,"NULL")
    }

  }



}
