package org.fin

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.fin.analyzer.Analyzer
import org.slf4j.{Logger, LoggerFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
/**
 * Hello world!
 *
 */
object FinancialAnalyzerPipeline{

  private lazy val LOG: Logger =
    LoggerFactory.getLogger(FinancialAnalyzerPipeline.getClass)

  def main(args: Array[String]): Unit = {
    val config: Config= ConfigFactory.load()
    val fapConfig:FinancialAnalyzerConfig =config.as[FinancialAnalyzerConfig]("fap")
    LOG.info(s"main config={}",fapConfig)
    System.setProperty("hadoop.home.dir", "F:\\software\\hadoop-2.8.3")
    val sparkSession: SparkSession =SparkSession.builder().appName("funds-analyzer").getOrCreate()
    val date="2020-06-30"
    try{
        LOG.info(s"main process started..")
        Analyzer.apply(fapConfig.analyzer, fapConfig).execute(sparkSession)
        LOG.info("main process ended...")
    }catch
    {
      case exception: Exception =>
        LOG.error(
          s"Unexpected error occurred while generating template!",
          exception
        )
      throw  exception
    }finally{
      sparkSession.stop()
    }


  }

}
