package de.leanovate.play.etcd

import org.joda.time.DateTime
import play.api.libs.json._

sealed trait EtcdNode

case class EtcdValueNode(
                          key: String,
                          value: String,
                          createdIndex: Option[Long],
                          modifiedIndex: Option[Long],
                          expiration: Option[DateTime],
                          ttl: Option[Long]
                          ) extends EtcdNode

case class EtcdDirNode(
                        key: String,
                        nodes: Seq[EtcdNode],
                        createdIndex: Option[Long],
                        modifiedIndex: Option[Long],
                        expiration: Option[DateTime],
                        ttl: Option[Long]
                        ) extends EtcdNode

object EtcdNode {
  implicit val jsonReads: Reads[EtcdNode] = new Reads[EtcdNode] {
    override def reads(json: JsValue): JsResult[EtcdNode] = {
      if ((json \ "dir").asOpt[Boolean].getOrElse(false)) {
        for {
          key <- (json \ "key").validate[String]
          nodes <- (json \ "nodes").validateOpt[Seq[EtcdNode]]
          createdIndex <- (json \ "createdIndex").validateOpt[Long]
          modifiedIndex <- (json \ "modifiedIndex").validateOpt[Long]
          expiration <- (json \ "expiration").validateOpt[DateTime]
          ttl <- (json \ "ttl").validateOpt[Long]
        } yield EtcdDirNode(key, nodes.toSeq.flatten, createdIndex, modifiedIndex, expiration, ttl)
      } else {
        for {
          key <- (json \ "key").validate[String]
          value <- (json \ "value").validate[String]
          createdIndex <- (json \ "createdIndex").validateOpt[Long]
          modifiedIndex <- (json \ "modifiedIndex").validateOpt[Long]
          expiration <- (json \ "expiration").validateOpt[DateTime]
          ttl <- (json \ "ttl").validateOpt[Long]
        } yield EtcdValueNode(key, value, createdIndex, modifiedIndex, expiration, ttl)
      }
    }
  }
}
