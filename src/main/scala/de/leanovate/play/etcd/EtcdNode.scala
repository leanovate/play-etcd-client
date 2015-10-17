package de.leanovate.play.etcd

import java.time.Instant
import play.api.libs.json._

sealed trait EtcdNode

case class EtcdValueNode(
                          key: String,
                          value: String,
                          createdIndex: Option[Long],
                          modifiedIndex: Option[Long],
                          expiration: Option[Instant],
                          ttl: Option[Long]
                          ) extends EtcdNode

case class EtcdDirNode(
                        key: String,
                        nodes: Seq[EtcdNode],
                        createdIndex: Option[Long],
                        modifiedIndex: Option[Long],
                        expiration: Option[Instant],
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
          expiration <- (json \ "expiration").validateOpt[Instant]
          ttl <- (json \ "ttl").validateOpt[Long]
        } yield EtcdDirNode(key, nodes.toSeq.flatten, createdIndex, modifiedIndex, expiration, ttl)
      } else {
        for {
          key <- (json \ "key").validate[String]
          value <- (json \ "value").validateOpt[String]
          createdIndex <- (json \ "createdIndex").validateOpt[Long]
          modifiedIndex <- (json \ "modifiedIndex").validateOpt[Long]
          expiration <- (json \ "expiration").validateOpt[Instant]
          ttl <- (json \ "ttl").validateOpt[Long]
        } yield EtcdValueNode(key, value.getOrElse(""), createdIndex, modifiedIndex, expiration, ttl)
      }
    }
  }
}
