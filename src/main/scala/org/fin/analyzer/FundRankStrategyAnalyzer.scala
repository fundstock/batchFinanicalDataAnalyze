package org.fin.analyzer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.fin.FinancialAnalyzerConfig
import org.fin.model.FunkRank
import org.slf4j.{Logger, LoggerFactory}

class FundRankStrategyAnalyzer(config: FinancialAnalyzerConfig) extends Analyzer(config) {
  override protected def LOG: Logger = LoggerFactory.getLogger(getClass)

  def cal4thRankMeasure(item: FunkRank):Int = {
    var measure=0
    /*
    (item.thisYearRank.trim.equals("良好") ||item.thisYearRank.trim.equals("优秀"))&&
      (item.thisWeekRank.trim.equals("良好")||item.thisWeekRank.trim.equals("优秀"))&&
      (item.thisMonthRank.trim.equals("良好")||item.thisMonthRank.trim.equals("优秀"))&&
      (item.threeMonthRank.trim.equals("良好")||item.threeMonthRank.trim.equals("优秀"))&&
      (item.sixMonthRank.trim.equals("良好")||item.sixMonthRank.trim.equals("优秀"))&&
      (item.oneYearRank.trim.equals("良好")||item.oneYearRank.trim.equals("优秀"))&&
      (item.twoYearRank.trim.equals("良好")||item.twoYearRank.trim.equals("优秀"))&&
      (item.threeYearRank.trim.equals("良好")||item.threeYearRank.trim.equals("优秀"))&&
      (item.fiveYearRank.trim.equals("良好")||item.fiveYearRank.trim.equals("优秀"))*/
    if(item.thisYearRank.trim.equals("一般")) measure=measure-1;
    if(item.thisYearRank.trim.equals("不佳")) measure=measure-2;
    if(item.thisYearRank.trim.equals("良好")) measure=measure+1;
    if(item.thisYearRank.trim.equals("优秀")) measure=measure+2;
    if(item.thisWeekRank.trim.equals("一般")) measure=measure-1;
    if(item.thisWeekRank.trim.equals("不佳")) measure=measure-2;
    if(item.thisWeekRank.trim.equals("良好")) measure=measure+1;
    if(item.thisWeekRank.trim.equals("优秀")) measure=measure+2;
    if(item.thisMonthRank.trim.equals("一般")) measure=measure-1;
    if(item.thisMonthRank.trim.equals("不佳")) measure=measure-2;
    if(item.thisMonthRank.trim.equals("良好")) measure=measure+1;
    if(item.thisMonthRank.trim.equals("优秀")) measure=measure+2;
    if(item.threeMonthRank.trim.equals("一般")) measure=measure-1;
    if(item.threeMonthRank.trim.equals("不佳")) measure=measure-2;
    if(item.threeMonthRank.trim.equals("良好")) measure=measure+1;
    if(item.threeMonthRank.trim.equals("优秀")) measure=measure+2;
    if(item.sixMonthRank.trim.equals("一般")) measure=measure-1;
    if(item.sixMonthRank.trim.equals("不佳")) measure=measure-2;
    if(item.sixMonthRank.trim.equals("良好")) measure=measure+1;
    if(item.sixMonthRank.trim.equals("优秀")) measure=measure+2;
    if(item.oneYearRank.trim.equals("一般")) measure=measure-1;
    if(item.oneYearRank.trim.equals("不佳")) measure=measure-2;
    if(item.oneYearRank.trim.equals("良好")) measure=measure+1;
    if(item.oneYearRank.trim.equals("优秀")) measure=measure+2;
    if(item.twoYearRank.trim.equals("一般")) measure=measure-1;
    if(item.twoYearRank.trim.equals("不佳")) measure=measure-2;
    if(item.twoYearRank.trim.equals("良好")) measure=measure+1;
    if(item.twoYearRank.trim.equals("优秀")) measure=measure+2;
    if(item.threeYearRank.trim.equals("一般")) measure=measure-1;
    if(item.threeYearRank.trim.equals("不佳")) measure=measure-2;
    if(item.threeYearRank.trim.equals("良好")) measure=measure+1;
    if(item.threeYearRank.trim.equals("优秀")) measure=measure+2;
    if(item.fiveYearRank.trim.equals("一般")) measure=measure-1;
    if(item.fiveYearRank.trim.equals("不佳")) measure=measure-2;
    if(item.fiveYearRank.trim.equals("良好")) measure=measure+1;
    if(item.fiveYearRank.trim.equals("优秀")) measure=measure+2;
    measure
  }

  override def execute(sparkSession: SparkSession): Unit = {
    val sparkContext: SparkContext = sparkSession.sparkContext
    import sparkSession.implicits._
    val path="G:\\financial\\crawler\\data\\fundraise_range\\"
    val fundRankRDD: RDD[(String,Int)] = sparkContext.wholeTextFiles(path).map(
      fileItem=>{buildFunkRank(fileItem)}).filter(item=>item.isDefined).map(item=> {
      val funk=item.get
      funk
    }).map(item=>{ (item.id,cal4thRankMeasure(item))
    }).sortBy(item=>item._2,false)
    val df=fundRankRDD.toDF().limit(50)
    df.write.mode(SaveMode.Overwrite).format("csv").save(path+"result.csv")
  }

  def buildFunkRank(fileItem:(String,String)) : Option[FunkRank] = {
    try{
          val fundID = fileItem._1.split("\\/").last.split("\\.").head
          val lineArray=fileItem._2.split('\n')
          val thisYear=lineArray(1).split(",")
          val thisYear2Hs300=thisYear(1).replace("%","").trim.toDouble-thisYear(3).replace("%","").trim.toDouble
          val thisYearRank=thisYear(5)
          val oneWeek=lineArray(2).split(",")
          val oneWeek2Hs300=oneWeek(1).replace("%","").trim.toDouble-oneWeek(3).replace("%","").trim.toDouble
          val oneWeekRank=oneWeek(5)
          val oneMonth=lineArray(3).split(",")
          val oneMonth2Hs300=oneMonth(1).replace("%","").trim.toDouble-oneMonth(3).replace("%","").trim.toDouble
          val oneMonthRank=oneMonth(5)
          val threeMonth=lineArray(4).split(",")
          val threeMonth2Hs300=threeMonth(1).replace("%","").trim.toDouble-threeMonth(3).replace("%","").trim.toDouble
          val threeMonthRank=threeMonth(5)
          val sixMonth=lineArray(5).split(",")
          val sixMonthHs300=sixMonth(1).replace("%","").trim.toDouble-sixMonth(3).replace("%","").trim.toDouble
          val sixMonthRank=sixMonth(5)
          val oneYear=lineArray(6).split(",")
          val oneYearHs300=oneYear(1).replace("%","").trim.toDouble-oneYear(3).replace("%","").trim.toDouble
          val oneYearRank=oneYear(5)
          val twoYear=lineArray(7).split(",")
          val twoYearHs300=twoYear(1).replace("%","").trim.toDouble-twoYear(3).replace("%","").trim.toDouble
          val twoYearRank=twoYear(5)
          val threeYear=lineArray(8).split(",")
          val threeYearHs300=threeYear(1).replace("%","").trim.toDouble-threeYear(3).replace("%","").trim.toDouble
          val threeYearRank=threeYear(5)
          val FiveYear=lineArray(9).split(",")
          val fiveYear2Hs300=FiveYear(1).replace("%","").trim.toDouble-FiveYear(3).replace("%","").trim.toDouble
          val fiveYearRank=FiveYear(5)
          Some(FunkRank(fundID.toString,thisYear2Hs300,thisYearRank,oneWeek2Hs300,oneWeekRank,
            oneMonth2Hs300,oneMonthRank,threeMonth2Hs300,threeMonthRank,sixMonthHs300,sixMonthRank,
            oneYearHs300,oneYearRank,twoYearHs300,twoYearRank,threeYearHs300,threeYearRank,fiveYear2Hs300,fiveYearRank))
    }catch{
      case e: Exception =>
        LOG.error(s"buildFunkRank Exception:${e.getMessage}")
        None
    }
  }
}
