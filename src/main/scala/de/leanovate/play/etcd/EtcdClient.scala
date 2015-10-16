package de.leanovate.play.etcd

import javax.inject.{Inject, Named}

import play.api.http.Status
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.Future

/**
 * Low level interactions with etcd server.
 *
 * @param etcdUrl Base url to the etcd server (usually http://localhost:2379)
 * @param wsClient WSClient implementation to use
 */
class EtcdClient @Inject()(
                            @Named("etdUrl") etcdUrl: String,
                            wsClient: WSClient
                            ) {
  def getNode(key: String,
              wait: Option[Boolean] = None,
              recursive: Option[Boolean] = None,
              sorted: Option[Boolean] = None,
              waitIndex: Option[Long] = None): Future[EtcdResult] = {
    val query =
      wait.map("wait" -> _.toString).toSeq ++
        recursive.map("recursive" -> _.toString).toSeq ++
        sorted.map("sorted" -> _.toString).toSeq ++
        waitIndex.map("waitIndex" -> _.toString).toSeq

    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString(query: _ *).get().map(handleResponse)
  }

  def updateDir(key: String, ttl: Option[Long] = None): Future[EtcdResult] = {
    val params = (Seq("dir" -> Seq("true")) ++
      ttl.map(v => "ttl" -> Seq(v.toString))
      ).toMap

    wsClient.url(s"$etcdUrl/v2/keys$key").put(params).map(handleResponse)
  }

  def deleteDir(key: String): Future[EtcdResult] = {
    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString("dir" -> "true").delete().map(handleResponse)
  }

  def createValue(dirKey: String, value: String, ttl: Option[Long] = None): Future[EtcdResult] = {
    val params = (Seq("value" -> Seq(value)) ++
      ttl.map(v => "ttl" -> Seq(v.toString))
      ).toMap

    wsClient.url(s"$etcdUrl/v2/keys$dirKey").post(params).map(handleResponse)
  }

  def updateValue(key: String, value: String,
                  ttl: Option[Long] = None,
                  prevValue: Option[String] = None,
                  prevIndex: Option[Long] = None,
                  prevExists: Option[Boolean] = None): Future[EtcdResult] = {
    val params = (Seq("value" -> Seq(value)) ++
      ttl.map(v => "ttl" -> Seq(v.toString))
      ).toMap
    val query = prevValue.map("prevValue" -> _).toSeq ++
      prevIndex.map("prevIndex" -> _.toString).toSeq ++
      prevExists.map("prevExists" -> _.toString).toSeq

    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString(query: _ *).put(params).map(handleResponse)
  }

  def deleteValue(key: String,
                  prevValue: Option[String] = None,
                  prevIndex: Option[Long] = None): Future[EtcdResult] = {
    val query = prevValue.map("prevValue" -> _).toSeq ++
      prevIndex.map("prevIndex" -> _.toString).toSeq

    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString(query: _ *).delete().map(handleResponse)
  }

  private def handleResponse(response: WSResponse): EtcdResult =
    (response.status, response.header("X-Etcd-Index").map(_.toLong)) match {
      case (Status.OK, Some(etcdIndex)) =>
        EtcdSuccess.fromJson(etcdIndex, response.json)
      case (_, Some(etcdIndex)) =>
        EtcdError.fromJson(etcdIndex, response.json)
      case _ => throw new RuntimeException("X-Etcd-Index header missing in response")
    }
}
