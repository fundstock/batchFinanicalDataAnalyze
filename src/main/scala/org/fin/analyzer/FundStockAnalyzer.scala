package org.fin.analyzer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.fin.FinancialAnalyzerConfig
import org.slf4j.{Logger, LoggerFactory}

class FundStockAnalyzer(config: FinancialAnalyzerConfig) extends Analyzer(config) {

  override protected def LOG: Logger =  LoggerFactory.getLogger(getClass)

  override def execute(sparkContext: SparkSession): Unit = {
    /**
    val dataPath: String =new File("G:\\financial\\data\\fundstocks").getPath
      val stockRDD: RDD[FundStatistic] = sparkContext.wholeTextFiles(dataPath).flatMap(fileItem=>{
        val stockArray: Array[FundStatistic] = fileItem._2.split("\\s").map(line=>{
          val item=line.split(",")
          val fundStock = FundStatistic(item(0), item(1),item(3), 1, List(fileItem._1))
          fundStock
        })
        stockArray
      })
      stockRDD.foreach(item=>{
        println(item.toString)
      })
      /*
      val file2ContentRDD: RDD[(String, String)] = value
      val fundStockRDD: RDD[(String,FundStatistic)] = file2ContentRDD.map(item => {
        val tuple = (item._1, item._2.split("\\s"))
        println(s"tuple=${tuple._2.mkString(",")}")
        tuple
      }
      ).map(
        item=> {
          val fundID: String = item._1.split("/").last.split("_").head
          val textContent: Array[FundStatistic] = item._2.map(item => {
              val line=item.split(",")

              println(s"line=${str.toString}")
              str
          } )
          textContent
        }
      ).flatMap(item=>item).map({
        item=>{
          println(s"code=${item.code},item=${item.toString}")
          (item.code,item)
        }
      }).distinct()
      val resultRDD: RDD[(String, FundStatistic)] = fundStockRDD.reduceByKey((x: FundStatistic, y: FundStatistic) => FundStatistic(x.code, x.name, x.sum + y.sum, x.refFunds ++ y.refFunds))
      val sortedCSVRDD: RDD[String] = resultRDD.map(item => item._2).sortBy(item => item,ascending = false)
        .map(item=>Seq(item.code,item.name,item.sum,item.refFunds.mkString("|")).mkString(","))
      val outPutPath: String =getClass.getResource("/").getPath+"stocks.csv"
      var csvContentList=List(Array("code","name","count","funds"))
      sortedCSVRDD.collect().foreach(item=>
        {
          val  content: Array[String] =item.split(",")
          csvContentList=csvContentList:+content
        }
      )
      ResourceUtils.writeCSV(outPutPath,csvContentList.asJava)
      //sortedCSVRDD.repartition(1).saveAsTextFile("file:///"+outPutPath)
      //import sparkSession.implicits._
      //val csvDF: DataFrame = sortedCSVRDD.repartition(1).toDF("code", "name", "count", "funds")
      //csvDF.write.option("header",true).option("encoding", "UTF-8").mode(SaveMode.Overwrite).csv(outPutPath)
     */

     */
  }
}
