package de.leanovate.play.etcd

import javax.inject.{Inject, Named}

import play.api.http.Status
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.ws.WSClient

import scala.concurrent.Future
import scala.util.Try

class EtcdClient @Inject()(
                            @Named("etdUrl") etcdUrl: String,
                            wsClient: WSClient
                            ) {
  def getNode(key: String,
              wait: Option[Boolean] = None,
              recursive: Option[Boolean] = None,
              sorted: Option[Boolean] = None,
              waitIndex: Option[Long] = None): Future[EtcdResult] = {
    wsClient.url(s"$etcdUrl/v2/keys$key").get().map {
      response =>
        response.status match {
          case Status.OK =>
            EtcdSuccess.fromJson(response.header("X-Etcd-Index").map(_.toLong), Try(response.json))
          case _ =>
            EtcdError.fromJson(response.header("X-Etcd-Index").map(_.toLong), Try(response.json))
        }
    }
  }

  def createDir(key: String, ttl: Option[Long] = None): Future[EtcdResult] = ???

  def createValue(dirKey: String, value: String, ttl: Option[Long] = None): Future[EtcdResult] = ???

  def updateValue(key: String, value: String,
                  ttl: Option[Long] = None,
                  prevValue: Option[String] = None,
                  prevIndex: Option[Long] = None,
                  prevExists: Option[Boolean] = None): Future[EtcdResult] = ???

  def deleteValue(key: String,
                  prevValue: Option[String] = None,
                  prevIndex: Option[Long] = None): Future[EtcdResult] = ???
}
