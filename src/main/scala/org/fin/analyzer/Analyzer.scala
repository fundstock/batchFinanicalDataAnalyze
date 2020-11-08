package org.fin.analyzer

import javassist.bytecode.stackmap.TypeData.ClassName
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.fin.FinancialAnalyzerConfig
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.universe._

/** This abstract class provides the skeleton for analyzer pipeline
 *
 */
abstract class Analyzer(val config: FinancialAnalyzerConfig) extends Serializable {

    protected  def LOG: Logger

    def execute(sparkContext: SparkSession): Unit

}

/**
 * This companion object is responsible to generate dynamically through reflection implementation of the
 * abstract super class Analyzer
 */
object Analyzer{

    private val LOG=LoggerFactory.getLogger(getClass)

   /**
   *
   * @param className
   * @param config
   * @return
   */
    def apply(className: String, config: FinancialAnalyzerConfig):Analyzer={
      LOG.debug(s"apply className=$className, config=$config")
      val fullClassName=s"${this.getClass.getPackage}.$className".substring(8)
      createInstance(fullClassName,config).asInstanceOf[Analyzer]
    }

   /**
   *
   * @param className
   * @param config
   * @return
   */
    private def createInstance(className: String, config: FinancialAnalyzerConfig): Any ={
        LOG.debug(s"createInstance className=$className, config=$config")
        val mirror=runtimeMirror(this.getClass.getClassLoader)

        // Determine the primary construct
        val classSymbol =mirror.classSymbol(Class.forName(className))
        val primaryConstructor = classSymbol.info.decls.find(m=>m.isMethod && m.asMethod.isPrimaryConstructor).head

        // Create instance
        val classMirror=mirror.reflectClass(classSymbol.asClass)
        classMirror.reflectConstructor(primaryConstructor.asMethod)(config)
    }

}
