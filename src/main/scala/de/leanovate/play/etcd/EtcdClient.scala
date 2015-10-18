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
  /**
   * Get a node by key.
   *
   * @param key The key
   * @param wait Optional wait for change
   * @param recursive Optional recursive for directory nodes
   * @param sorted Optional sorted for directory nodes
   * @param waitIndex Optional waitIndex (in combination with `wait`)
   * @return Result from etcd cluster
   */
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

  /**
   * Create of update a directory node.
   *
   * @param key The key of the directory node
   * @param ttl Optional time to live for the node
   * @return Result from etcd cluster
   */
  def updateDir(key: String, ttl: Option[Long] = None): Future[EtcdResult] = {
    val params = (Seq("dir" -> Seq("true")) ++
      ttl.map(v => "ttl" -> Seq(v.toString))
      ).toMap

    wsClient.url(s"$etcdUrl/v2/keys$key").put(params).map(handleResponse)
  }

  /**
   * Delete a directory node.
   *
   * @param key The key of the directory node
   * @return Result from etcd cluster
   */
  def deleteDir(key: String): Future[EtcdResult] = {
    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString("dir" -> "true").delete().map(handleResponse)
  }

  /**
   * Create a value node in a directory.
   *
   * Key of the node is uniquely generated by the etcd cluster.
   *
   * @param dirKey The key of the directory node
   * @param value The value to store
   * @param ttl Optional time to live for the value node
   * @return Result from etcd cluster
   */
  def createValue(dirKey: String, value: String, ttl: Option[Long] = None): Future[EtcdResult] = {
    val params = (Seq("value" -> Seq(value)) ++
      ttl.map(v => "ttl" -> Seq(v.toString))
      ).toMap

    wsClient.url(s"$etcdUrl/v2/keys$dirKey").post(params).map(handleResponse)
  }

  /**
   * Create or update a value node.
   *
   * @param key The key of the value node
   * @param value The value to store
   * @param ttl Optional time to live for the value node
   * @param prevValue Optional previous value of the node (compare and set operation)
   * @param prevIndex Optional previous index of the node (compare and set operation)
   * @param prevExist Optional flag if node is supposed to exists (compare and set operation)
   * @return Result from etcd cluster
   */
  def updateValue(key: String, value: String,
                  ttl: Option[Long] = None,
                  prevValue: Option[String] = None,
                  prevIndex: Option[Long] = None,
                  prevExist: Option[Boolean] = None): Future[EtcdResult] = {
    val params = (Seq("value" -> Seq(value)) ++
      ttl.map(v => "ttl" -> Seq(v.toString))
      ).toMap
    val query = prevValue.map("prevValue" -> _).toSeq ++
      prevIndex.map("prevIndex" -> _.toString).toSeq ++
      prevExist.map("prevExist" -> _.toString).toSeq

    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString(query: _ *).put(params).map(handleResponse)
  }

  /**
   * Delete a value node.
   *
   * @param key The key of the value node
   * @param prevValue Optional previous value of the node (compare and set operation)
   * @param prevIndex Optional previous index of the node (compare and set operation)
   * @return Result from etcd cluster
   */
  def deleteValue(key: String,
                  prevValue: Option[String] = None,
                  prevIndex: Option[Long] = None): Future[EtcdResult] = {
    val query = prevValue.map("prevValue" -> _).toSeq ++
      prevIndex.map("prevIndex" -> _.toString).toSeq

    wsClient.url(s"$etcdUrl/v2/keys$key").withQueryString(query: _ *).delete().map(handleResponse)
  }

  private def handleResponse(response: WSResponse): EtcdResult =
    (response.status, response.header("X-Etcd-Index").map(_.toLong)) match {
      case (Status.OK | Status.CREATED, Some(etcdIndex)) =>
        EtcdSuccess.fromJson(etcdIndex, response.json)
      case (_, Some(etcdIndex)) =>
        EtcdError.fromJson(etcdIndex, response.json)
      case _ => throw new RuntimeException("X-Etcd-Index header missing in response")
    }
}
