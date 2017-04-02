package com.webanalytics.config
import java.text._
import java.util.Locale

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigSyntax}
import com.webanalytics.helper.Utilities
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
/**
  * Created by Thanas Koka on 21/02/2017.
  */
trait DataPreparation {
   var  basePath= "C:/zeppelin-0.6.2-bin-all/bin/"
   //var basePath= "wasb://tesi@datasettesi.blob.core.windows.net/data/"
   var OutputPath=basePath+"Output/"
   var IntervalAnalysis=Array(1,60,720,1440)
   var historyAnalysis=false

  val WebModelInputPath = "data/WebModel"
  val DataModelInputPath = "data/DataModel"
  val PersistentDataInputPath = "data/DbIstance"
  val ApacheLogInputPath =  "data/dataset-20161216"
  val RtxLogInputPath = "data/dataset-20161216"
  var statisticTypePath="data/statisticType.csv"
  //define Sources Cluster blob storage path
  var WebModelpath = basePath + WebModelInputPath + "/**/*/*/page*.wr," + basePath + WebModelInputPath +"/**/*/page*.wr," + basePath +WebModelInputPath + "/**/mpage*.wr," + basePath + WebModelInputPath +"/**/*/*/Properties.wr," + basePath +WebModelInputPath + "/**/*/Properties.wr"
  var DataModelpath = basePath + DataModelInputPath + "/Properties.wr"
  var PersistentDatapath = basePath + PersistentDataInputPath + "/"
  var ApacheLogPath = basePath + ApacheLogInputPath + "/localhost*.txt"
  var RtxLogPath = basePath + RtxLogInputPath + "/RTX.*"

  var  EnrichedLogsPath="/data/FinalEnrichedLogs.csv"
  var  ApacheDateFormat: SimpleDateFormat  = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  var  DateFormat: SimpleDateFormat  = new SimpleDateFormat("dd MMM yyyy HH:mm:ss", Locale.ENGLISH)


  var Units4Att = Array("PowerIndexUnit","IndexUnit","HierarchicalIndexUnit", "DataUnit","MultiDataUnit","EventCalendarUnit","MultiChoiceIndexUnit")

  var Units2Att=Array("ModuleInstanceUnit","MultiMessageUnit","NoOpContentUnit","TimeUnit","GetUnit","NoOpContentUnit")
  var Units3Att=Array("SelectorUnit","EntryUnit","MultiEntryUnit")
  var DbNames=Array("AUTHOR","BOOK","BOOK_AUTHOR","BOOK_RELATED_BOOK","CATEGORY","CATEGORY_RELATED_CATEGORY","COMMENT","EDITORIAL_REVIEW","GROUP","GROUP_MODULE","MODULE","ORDER","ORDER_ITEM","USER","USER_GROUP")
  var Pages = Array("Page","MasterPage")
  var oldTimestamp: Long=0

  def loadConfiguration(sc: SparkContext, sqlContext: SQLContext, args: Array[String]): Unit = {
    val properties_file_path = args(0)

    val conf_file = sc.textFile(properties_file_path)
    val sb: StringBuffer = new StringBuffer()
    for (i <- 0 to conf_file.collect().length - 1) {
      sb.append(conf_file.collect()(i) + "\n")
    }
    val conf_file_str: String = sb.toString

    val configOptions: ConfigParseOptions = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)

    val config: Config = ConfigFactory.parseString(conf_file_str, configOptions)

    basePath = config.getString("BasePath")

    EnrichedLogsPath=config.getString("EnrichedLogsPath")

    OutputPath=basePath+config.getString("OutputPath")

    val interval=config.getString("IntervalAnalysis")
    IntervalAnalysis=interval.split(",").map(_.toInt)

    statisticTypePath=basePath+config.getString("statisticType")
    historyAnalysis=config.getBoolean("historyAnalysis")
  }

  def readParameters(args: Array[String],sqlContext: SQLContext): Unit = {
    /* TO BE USED WHEN THE LOGS AND THE MODELS SHOULD BE PARSED BEFORE ANALYZED

        val containerName = args(0)
        val blobStorageName = args(1)
        val WebModelInputPath = args(2)
        val DataModelInputPath = args(3)
        val PersistentDataInputPath = args(4)
        val ApacheLogInputPath = args(5)
        val RtxLogInputPath = args(6)
        val OutputParamPath = args(7)

        //define Primary Storage Cluster base path
        var basePath = "wasb://" + containerName + "@" + blobStorageName + ".blob.core.windows.net/"
  */
      //define Sources Cluster blob storage path
  //    WebModelpath = basePath + WebModelInputPath + "/**/*/*/page*.wr," + basePath + WebModelInputPath +"/**/*/page*.wr," + basePath +WebModelInputPath + "/**/mpage*.wr," + basePath + WebModelInputPath +"/**/*/*/Properties.wr," + basePath +WebModelInputPath + "/**/*/Properties.wr"
    //  DataModelpath = basePath + DataModelInputPath + "/Properties.wr"
     // PersistentDatapath = basePath + PersistentDataInputPath + "/"
     // ApacheLogPath = basePath + ApacheLogInputPath + "/localhost*.txt"
      //RtxLogPath = basePath + RtxLogInputPath + "/RTX.*"
      //OutputPath = basePath + OutputParamPath + "/"



      try {
        oldTimestamp = Utilities.readLastTimestampIngestion(sqlContext)
      } catch {
        case e => {
          oldTimestamp = 0
        }
      }


  }

}
