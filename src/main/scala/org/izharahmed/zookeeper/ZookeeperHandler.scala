package org.izharahmed.zookeeper

import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat

/**
  * Created by Izhar Ahmed on 28-Dec-2017
  */
class ZookeeperHandler(connectString: String,
                       timeout: Int,
                       watcherOption: Option[Watcher] = None) {

  private val _watcher = if (watcherOption.isDefined) {
    watcherOption.get
  } else {
    new Watcher {
      override def process(event: WatchedEvent): Unit = Unit
    }
  }
  private val _zookeeper = new ZooKeeper(connectString, timeout, _watcher)

  /** Returns the ZooKeeper object
    *
    * @return
    */
  def getZooKeeperClient: ZooKeeper = _zookeeper

  /** Returns the Watcher object
    *
    * @return
    */
  def getZooKeeperWatcher: Watcher = _watcher

  implicit class ZooKeeperUtils(zooKeeper: ZooKeeper) {

    /** Check existence of path in zookeeper
      *
      * @param path input path
      * @return Option of status
      */
    def checkPath(path: String): Option[Stat] =
      Option(zooKeeper.exists(path, true))

    /** Read offset stored at path provided.
      *
      * @param path zookeeper path
      * @return Option of offset
      */
    def readOffSet(path: String): Option[Long] = {
      val pathExistsOption = checkPath(path)

      val data = pathExistsOption
        .map { stat =>
          val byteArray = zooKeeper.getData(path, false, stat)

          byteArray.map(_.toChar).mkString.toLong
        }

      data
    }

    /** Write offset at the path provided
      *
      * @param path zookeeper path
      * @param offSet offset value
      */
    def writeOffSet(path: String, offSet: Long): Unit = {
      val byteArray = offSet.toString.toCharArray.map(_.toByte)
      val acl = ZooDefs.Ids.OPEN_ACL_UNSAFE

      val offsetOption = checkPath(path)

      if (offsetOption.isEmpty) {
        zooKeeper.create(path, byteArray, acl, CreateMode.PERSISTENT)
      } else {
        zooKeeper.setData(path, byteArray, -1)
      }
    }
  }

}
