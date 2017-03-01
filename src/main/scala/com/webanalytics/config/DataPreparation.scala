package com.webanalytics.config

import java.text._
import java.util.Locale
/**
  * Created by Thanas Koka on 21/02/2017.
  */
trait DataPreparation {
  var WebModelpath: String="data/WebModel/**/*/*/page*.wr,data/WebModel/**/*/page*.wr,data/WebModel/**/mpage*.wr,data/WebModel/**/*/*/Properties.wr,data/WebModel/**/*/Properties.wr"

  var DataModelpath="data/DataModel/Properties.wr"

  var PersistentDatapath="data/DbIstance/"

  var ApacheLogPath="data/dataset-20161216/localhost*.txt"

  var RtxLogPath="data//dataset-20161216/RTX.*"

  var  ApacheDateFormat: SimpleDateFormat  = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  var  DateFormat: SimpleDateFormat  = new SimpleDateFormat("dd MMM yyyy HH:mm:ss", Locale.ENGLISH)

}
