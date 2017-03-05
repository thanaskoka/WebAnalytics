package com.webanalytics.config
import com.webanalytics.helper._
import java.text._
import java.util.Locale
import org.apache.spark.sql.SQLContext
/**
  * Created by Thanas Koka on 21/02/2017.
  */
trait DataPreparation {
  // var  basePath= "C:/zeppelin-0.6.2-bin-all/bin/"
  var basePath= "wasb://tesi@datasettesi.blob.core.windows.net/"
   var WebModelpath = basePath  + "data/WebModel/**/*/*/page*.wr," + basePath  +"data/WebModel/**/*/page*.wr," + basePath  + "data/WebModel/**/mpage*.wr," + basePath  +"data/WebModel/**/*/*/Properties.wr," + basePath  + "data/WebModel/**/*/Properties.wr"
   var  DataModelpath=basePath+"data/DataModel/Properties.wr"
   var  PersistentDatapath=basePath+"data/DbIstance/"
   var  ApacheLogPath=basePath+"data/dataset-20161216/localhost*.txt"
   var RtxLogPath=basePath+"data/dataset-20161216/RTX.*"
   var OutputPath=basePath+"Output/"




  var  ApacheDateFormat: SimpleDateFormat  = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  var  DateFormat: SimpleDateFormat  = new SimpleDateFormat("dd MMM yyyy HH:mm:ss", Locale.ENGLISH)


  var Units4Att = Array("PowerIndexUnit","IndexUnit","HierarchicalIndexUnit", "DataUnit","MultiDataUnit","EventCalendarUnit","MultiChoiceIndexUnit")

  var Units2Att=Array("ModuleInstanceUnit","MultiMessageUnit","NoOpContentUnit","TimeUnit","GetUnit","NoOpContentUnit")
  var Units3Att=Array("SelectorUnit","EntryUnit","MultiEntryUnit")
  var DbNames=Array("AUTHOR","BOOK","BOOK_AUTHOR","BOOK_RELATED_BOOK","CATEGORY","CATEGORY_RELATED_CATEGORY","COMMENT","EDITORIAL_REVIEW","GROUP","GROUP_MODULE","MODULE","ORDER","ORDER_ITEM","USER","USER_GROUP")
  var Pages = Array("Page","MasterPage")
  var oldTimestamp: Long=0


  def readParameters(args: Array[String],sqlContext: SQLContext): Unit = {

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

      //define Sources Cluster blob storage path
      WebModelpath = basePath + WebModelInputPath + "/**/*/*/page*.wr," + basePath + WebModelInputPath +"/**/*/page*.wr," + basePath +WebModelInputPath + "/**/mpage*.wr," + basePath + WebModelInputPath +"/**/*/*/Properties.wr," + basePath +WebModelInputPath + "/**/*/Properties.wr"
      DataModelpath = basePath + DataModelInputPath + "/Properties.wr"
      PersistentDatapath = basePath + PersistentDataInputPath + "/"
      ApacheLogPath = basePath + ApacheLogInputPath + "/localhost*.txt"
      RtxLogPath = basePath + RtxLogInputPath + "/RTX.*"
      OutputPath = basePath + OutputParamPath + "/"

      try {
        oldTimestamp = Utilities.readLastTimestampIngestion(sqlContext)
      } catch {
        case e => {
          oldTimestamp = 0
        }
      }


  }

}
