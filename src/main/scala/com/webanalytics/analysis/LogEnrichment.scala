package com.webanalytics.analysis

import com.webanalytics.config._
import org.apache.spark.SparkContext

/**
  * Created by Thanas Koka on 21/02/2017.
  */
object LogEnrichment extends DataPreparation {


  def main(args: Array[String]): Unit = {

/*  //run as spark standalone mode
    val conf= new SparkConf().setAppName("AutobusAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
*/
    val sc = new SparkContext()

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    readParameters(args)


  }

  def readParameters(args: Array[String]): Unit ={

    if(args.length>2) {
      val containerName = args(0)
      val blobStorageName = args(1)
      val inputDataPath = args(2)
      val outputDataPath = args(3)

      //define input and output Cluster blob storage path
      WebModelpath = "wasb://" + containerName + "@" + blobStorageName + ".blob.core.windows.net/" + inputDataPath + "/doc*.txt"
      DataModelpath = "wasb://" + containerName + "@" + blobStorageName + ".blob.core.windows.net/" + outputDataPath + "/"
    }
    else{
      val containerName = args(0)
      val blobStorageName = args(1)
      val inputDataPath = args(2)
      val outputDataPath = args(3)
      WebModelpath = "wasb://" + containerName + "@" + blobStorageName + ".blob.core.windows.net/" + inputDataPath + "/doc*.txt"
      DataModelpath = "wasb://" + containerName + "@" + blobStorageName + ".blob.core.windows.net/" + outputDataPath + "/"

    }
  }


}
