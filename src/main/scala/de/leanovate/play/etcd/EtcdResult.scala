package de.leanovate.play.etcd

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

sealed trait EtcdResult

case class EtcdSuccess(
                        etcdIndex: Option[Long],
                        action: String,
                        node: EtcdNode,
                        prevNode: Option[EtcdNode]
                        ) extends EtcdResult

object EtcdSuccess {
  private val jsonPaths =
    (JsPath \ "action").read[String] and
      (JsPath \ "node").read[EtcdNode] and
      (JsPath \ "prevNode").readNullable[EtcdNode]

  def fromJson(etcdIndex: Option[Long], jsonTry: Try[JsValue]): EtcdResult = jsonTry match {
    case Success(json) =>
      jsonPaths.apply(EtcdSuccess(etcdIndex, _, _, _)).reads(json) match {
        case JsSuccess(success, _) => success
        case JsError(errors) => EtcdFailure(new RuntimeException(s"Invalid json: ${errors}"))
      }
    case Failure(cause) => EtcdFailure(cause)
  }
}

case class EtcdError(
                      etcdIndex: Option[Long],
                      cause: String,
                      errorCode: Int,
                      index: Int,
                      message: String
                      ) extends EtcdResult

object EtcdError {
  private val jsonPaths =
    (JsPath \ "cause").read[String] and
      (JsPath \ "errorCode").read[Int] and
      (JsPath \ "index").read[Int] and
      (JsPath \ "message").read[String]

  def fromJson(etcdIndex: Option[Long], jsonTry: Try[JsValue]): EtcdResult = jsonTry match {
    case Success(json) =>
      jsonPaths.apply(EtcdError(etcdIndex, _, _, _, _)).reads(json) match {
        case JsSuccess(error, _) => error
        case JsError(errors) => EtcdFailure(new RuntimeException(s"Invalid json: ${errors}"))
      }
    case Failure(cause) => EtcdFailure(cause)
  }
}

case class EtcdFailure(
                        cause: Throwable
                        ) extends EtcdResult
