package org.fin.util

import java.io.{BufferedReader, BufferedWriter, FileOutputStream, FileWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import au.com.bytecode.opencsv.CSVWriter
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/** This class manages some helper functions.
  *
  */
object ResourceUtils {
  private lazy val LOG = LoggerFactory.getLogger(getClass)


  def writeCSV(path:String,content:java.util.List[Array[String]])={
    val outputFile = new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8) //new BufferedWriter(new FileWriter(path)) //replace the path with the desired path and filename with the desired filename
    val csvWriter = new CSVWriter(outputFile)
    csvWriter.writeAll(content)
    csvWriter.close()
  }

  /**
   * get the files list in the directory.
   * @param dir
   * @return
   */
  def getFiles(dir: String) : Seq[String] = {
    LOG.debug(s"readEvents dir=$dir")
    val inputResource = getClass.getClassLoader.getResourceAsStream(s"$dir")
    val filenames: Seq[String] = autoClose(inputResource) { r =>
      var files: Seq[String] = Seq.empty
      val br = new BufferedReader(new InputStreamReader(r))

      var resource: String = br.readLine
      while (resource != null) {
        files = files :+ resource
        resource = br.readLine
      }
      files
    }
    filenames
  }

  /** Function to open, read out and the close a file
   *
   * @param resource the input file
   * @param block    the block
   * @tparam A the resource type
   * @tparam B the resource return type
   * @return the result
   */
  private def autoClose[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }



}