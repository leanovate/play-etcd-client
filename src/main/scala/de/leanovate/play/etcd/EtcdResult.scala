package de.leanovate.play.etcd

import play.api.libs.json._

sealed trait EtcdResult

case class EtcdSuccess(
                        etcdIndex: Long,
                        action: String,
                        node: EtcdNode,
                        prevNode: Option[EtcdNode]
                        ) extends EtcdResult

object EtcdSuccess {
  def fromJson(etcdIndex: Long, json: JsValue): EtcdResult = {
    EtcdSuccess(
      etcdIndex,
      (json \ "action").as[String],
      (json \ "node").as[EtcdNode],
      (json \ "prevNode").asOpt[EtcdNode]
    )
  }
}

case class EtcdError(
                      etcdIndex: Long,
                      cause: String,
                      errorCode: Int,
                      index: Int,
                      message: String
                      ) extends EtcdResult

object EtcdError {
  def fromJson(etcdIndex: Long, json: JsValue): EtcdResult = {
    EtcdError(
      etcdIndex,
      (json \ "cause").as[String],
      (json \ "errorCode").as[Int],
      (json \ "index").as[Int],
      (json \ "message").as[String]
    )
  }
}
