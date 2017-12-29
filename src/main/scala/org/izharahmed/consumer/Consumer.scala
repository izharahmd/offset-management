package org.izharahmed.consumer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.izharahmed.zookeeper.ZookeeperHandler
import org.izharahmed.configuration.{GlobalConfiguration => GC}
import org.izharahmed.producer.{InputData, Producer}

import scala.reflect.ClassTag
import scala.util.Try

case class TransformResult[B](status: Boolean, newOffset: Option[Long], resultRDD: RDD[B])

/** Entry point
  * Created by Izhar Ahmed on 28-Dec-2017
  */
object Consumer {

  private val logger = GC.logger

  private lazy val _zookeeperHandler = new ZookeeperHandler(getConnectString, 5000)
  import _zookeeperHandler.ZooKeeperUtils

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OffsetManagement")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    // read count of rows to be processed from arguments
    val chunkSize: Int = getChunkSizeFromArgs(args)

    // consume data from offset
    val fetchData = consume(chunkSize, sqlContext)

    // perform transformation
    val transformResultOption = transform[InputData](fetchData, sqlContext)

    // update offset based on transformation status
    transformResultOption match {
      case Some(TransformResult(true, Some(offset), rdd)) =>
        setOffset(offset)
        writeTransformationRDD(rdd, offset, sqlContext) // writing test data
      case Some(TransformResult(true, None, _)) => logger.warn("Offset not updated")
      case None                                 => Unit
    }

  }

  /** Initiate the producer to produce data
    *
    * @param chunkSize Size of rows to process
    * @param sqlContext SQL Context
    * @return rdd of [`InputData`]
    */
  def consume(chunkSize: Int, sqlContext: SQLContext): RDD[InputData] = {
    val producer = new Producer(sqlContext)
    val chunkedData = producer.produce(chunkSize)

    chunkedData
  }

  /** Processes Transforms the input RDD. Various transformation logic can be
    * plugged in here.
    *
    * @param rdd input rdd
    * @return the status of the transformation and the max timestamp processed
    */
  def transform[T: ClassTag](rdd: RDD[InputData],
                             sqlContext: SQLContext): Option[TransformResult[T]] = {
    val resultRDD = try {
      //some transformations
      val transformedRDD = rdd.map(_.asInstanceOf[T])
      val c = rdd.count()
      logger.info(s"Processed $c rows")
      transformedRDD
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return None
    }
    val newOffset = getMaxTimeStamp(rdd)

    Some(TransformResult[T](status = true, newOffset, resultRDD))
  }

  /** Returns the zookeeper path where offset is stored
    *
    * @return
    */
  def getZkPath: String = s"""/${GC.jobId}"""

  /** Returns the zookeeper connect string
    *
    * @return connection string
    */
  def getConnectString: String = s"${GC.zooKeeperHost}:${GC.zooKeeperPort}"

  /** Getter method to return zookeeper handler object
    *
    * @return ZookeeperHandler object
    */
  def getZooKeeperHandler: ZookeeperHandler = _zookeeperHandler

  /** Return the max timestamp which has been processed
    *
    * @param rdd chunked rdd
    * @return Option of the max timestamp processed and None when input is empty
    */
  def getMaxTimeStamp(rdd: RDD[InputData]): Option[Long] = {
    implicit def ordered: Ordering[InputData] = new Ordering[InputData] {
      def compare(x: InputData, y: InputData): Int = x.timestamp compareTo y.timestamp
    }

    val maxDoc = try {
      Some(rdd.max())
    } catch {
      case e: UnsupportedOperationException =>
        if (e.getMessage == "empty collection") logger.info("No data to process")
        None
    }
    val timeStampOption = maxDoc.map(_.timestamp)

    timeStampOption
  }

  /** Updates the offset value in zookeeper data node
    *
    * @param offset timestamp value
    */
  def setOffset(offset: Long): Unit = {
    val zKClient = _zookeeperHandler.getZooKeeperClient

    zKClient.writeOffSet(getZkPath, offset)
    logger.info(s"Offset updated to $offset")
  }

  /** Parses the input arguments
    *
    * @param args command line arguments array
    * @return chunk size Integer value
    */
  def getChunkSizeFromArgs(args: Array[String]): Int = {
    val argsList = args.toList
    val chunkSizeIndex = argsList.indexOf("-chunkSize")

    val chunkSize = chunkSizeIndex match {
      case -1 =>
        throw new UnsupportedOperationException(
          s"Incorrect input  arguments - $argsList. chunkSize missing")
      case i: Int =>
        Try(argsList(i + 1).toInt).toOption
          .getOrElse(throw new IllegalArgumentException("Valid chunk size value not specified"))
    }

    chunkSize
  }

  /** Write the transformation result RDD as CSV in the tes/resources folder
    *
    * NOTE: This method is only used for testing purpose and the actual LOAD part
    * of the result should be handled separately.
    *
    * @param rdd Input RDD
    * @param offset offset value used to create path
    * @param sqlContext SQL Context
    */
  def writeTransformationRDD(rdd: RDD[InputData], offset: Long, sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._

    rdd
      .toDF()
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"src/test/resources/${GC.jobId}_$offset")
  }

}
