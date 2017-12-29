package org.izharahmed.producer

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.expressions.Window
import org.izharahmed.configuration.GlobalConfiguration
import org.izharahmed.consumer.Consumer

import scala.util.Try

case class InputData(id: String, firstName: String, lastName: String, timestamp: Long)

/**
  * Created by Izhar Ahmed on 29-Dec-2017
  */
class Producer(sqlContext: SQLContext) {

  /** Entry point for the Producer object. This method is responsible of reading
    * the input source based on the offset stored in ZooKeeper.
    *
    * Currently spark-sql is being used to read data.
    *
    * @param size Chunk size of records to produce. This is the max size that should
    *             be produced.
    * @return RDD[InputData]
    */
  def produce(size: Int): RDD[InputData] = {
    val timestamp = GlobalConfiguration.timeStampColumn
    val inputConfig = GlobalConfiguration.applicationConfig.getConfig("input")
    val inputDF = readSourceToDataFrame(inputConfig, sqlContext)

    val offsetOption = this.getZooKeeperOffset
    GlobalConfiguration.logger.info(s"Initial offset - ${offsetOption.orNull}")

    // TODO: Streaming can be implemented here
    val window = Window.orderBy(col(timestamp))
    val timestampFilteredDF = offsetOption match {
      case Some(offsetTimestamp) => inputDF.filter(col(timestamp) > offsetTimestamp)
      case None                  => inputDF //first run case, no offset
    }

    val chunkDF = timestampFilteredDF
      .withColumn("rn", row_number.over(window))
      .where(col("rn") <= size)
      .drop("rn")

    // convert DataFrame to RDD
    convertToInputData(chunkDF)
  }

  /** Method to convert a dataframe to an RDD[InputData].
    * Logic of how to process the data is defined here. If the schema of input
    * source changes then the below logic should be updated
    *
    * @param df input DataFrame
    * @return RDD[InputData]
    */
  private def convertToInputData(df: DataFrame): RDD[InputData] = {
    val rdd = df.rdd.map { row =>
      val id = row.getString(0)
      val fn = row.getString(1)
      val ln = row.getString(2)
      val timestamp = row.getString(3).toLong

      InputData(id = id, firstName = fn, lastName = ln, timestamp = timestamp)
    }

    rdd
  }

  /** Helper method to read various data sources. This is the entry point for reading
    * any source. If a new source support is added, its entry should be made over here
    *
    * @param config read config
    * @param sqlContext SQL Context
    * @return a DataFrame
    */
  private def readSourceToDataFrame(config: Config, sqlContext: SQLContext): DataFrame = {
    val sourceType = config.getString("type")

    sourceType match {
      case "csv"   => readCSV(sqlContext, config.getConfig("options"))
      case "mysql" => readMySQL(sqlContext, config.getConfig("options"))
      case _ =>
        throw new IllegalArgumentException(s"Source type $sourceType not supported")
    }
  }

  /** Read the input csv
    *
    * @param sqlContext SQL Context
    * @param optionConfig option config with csv options
    * @return a DataFrame
    */
  private def readCSV(sqlContext: SQLContext, optionConfig: Config): DataFrame = {
    val header =
      Try(optionConfig.getString("header")).toOption.getOrElse("false")

    val path = optionConfig.getString("path")
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", header)
      .load(path)

    df
  }

  private def readMySQL(sqlContext: SQLContext, optionConfig: Config): DataFrame = ???

  /** Read the offset present in zookeeper data node. The zookeeper path is
    * present in the application.conf
    *
    * @return Option of offset. If path does not exists then None.
    */
  private def getZooKeeperOffset: Option[Long] = {
    val zkHandler = Consumer.getZooKeeperHandler
    val zKClient = zkHandler.getZooKeeperClient
    val zkPath = Consumer.getZkPath

    import zkHandler.ZooKeeperUtils // import

    val offsetOption = zKClient.readOffSet(zkPath)

    offsetOption
  }

}
