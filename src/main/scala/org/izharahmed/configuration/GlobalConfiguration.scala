package org.izharahmed.configuration

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{LogManager, Logger}

/**
  * Created by Izhar Ahmed on 29-Dec-2017
  */
object GlobalConfiguration {

  val logger:Logger = LogManager.getLogger("OffsetManagement")

  val applicationConfig: Config = ConfigFactory.load()
  val zooKeeperHost: String = applicationConfig.getString("zooKeeperHost")
  val zooKeeperPort: Int = applicationConfig.getInt("zooKeeperPort")

  /*
  timestamp is used to maintain offset in zookeeper
   */
  val timeStampColumn: String = applicationConfig.getString("timeStampColumn")

  /*
  jobId is used to maintain path in zookeeper data node
   */
  val jobId: String = applicationConfig.getString("jobId")

}
