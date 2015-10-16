package de.leanovate.play.etcd

import org.joda.time.DateTime
import play.api.libs.json.Json

case class EtcdNode(
                     key: Option[String],
                     value: Option[String],
                     dir: Option[Boolean],
                     nodes: Seq[EtcdNode],
                     createdIndex: Option[Long],
                     modifiedIndex: Option[Long],
                     expiration: Option[DateTime],
                     ttl: Option[Long]
                     )


object EtcdNode {
  implicit val jsonReads = Json.reads[EtcdNode]
}
