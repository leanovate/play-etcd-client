package de.leanovate.play.etcd

import javax.inject.Inject

import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.Future

/**
 * Collection of patterns one might use in combination with etcd
 */
class EtcdOperations @Inject()(
                                etcdClient: EtcdClient
                                ) {
  /**
   * Get value helper.
   *
   * Value nodes will result in a sequence with one element.
   * If node does not exists, result will be empty.
   * if it is a directory a the values of all child value nodes are returned
   *
   * @param key The key to lookup
   * @return All values found at `key`
   */
  def getValues(key: String): Future[Seq[String]] = ???

  /**
   * Tries an atomic transformation of a value node.
   *
   * The transformation is only tried once and might fail.
   *
   * @param key The key of the node to transform
   * @param transformation The transformation
   * @return The current value of the and if the transformation was successful
   */
  def tryTransformValue(key: String, transformation: (String) => String): Future[(Boolean, String)] =
    etcdClient.getNode(key).map(valueFromResult).flatMap {
      value =>
        etcdClient.updateValue(key, transformation(value), prevValue = Some(value)).flatMap {
          case EtcdSuccess(_, _, EtcdValueNode(_, newValue, _, _, _, _), _) => Future.successful((true, newValue))
          case _ => etcdClient.getNode(key).map(valueFromResult).map((false, _))
        }
    }

  /**
   * Perform an atomic transformation of a value node.
   *
   * @param key The key of the node to transform
   * @param transformation The transformation (might be called multiple times, depending how many other clients
   *                       are trying to do this)
   * @return The value after the transformation
   */
  def transformValue(key: String, transformation: (String) => String): Future[String] =
    tryTransformValue(key, transformation).flatMap {
      case (true, value) => Future.successful(value)
      case _ => transformValue(key, transformation)
    }

  /**
   * Enqueue a value when (mis)using etcd as queueing service.
   *
   * The queue is supposed to behave like a fifo and ony once client is supposed to receive the value.
   *
   * @param dirKey the directory node to use for queuing
   * @param ttl Optional time to live of the value
   * @return the key of the enqueued node
   */
  def enqueueValue(dirKey: String, ttl: Option[Long]): Future[String] = ???

  /**
   * Dequeue a value when (mis)using etcd as queueing service.
   *
   * The queue is supposed to behave like a fifo and ony once client is supposed to receive the value.
   *
   * @param dirKey the directory node to use for queuing
   * @return dequed value
   */
  def dequeueValue(dirKey: String): Future[String] = ???

  /**
   * Lock a code block via etcd.
   *
   * I.e. only one clinet is supposed to run a `block` at a time.
   *
   * @param key The key to use for locking (will become a value key)
   * @param ttl Optional time to live of the key (recomented to prevent deadlocks in case of a major failure)
   * @param block The code block requiring cluster-wide synchronization
   */
  def lock(key: String, ttl: Option[Long])(block: => Unit): Unit = ???

  private def valueFromResult(result: EtcdResult): String = result match {
    case EtcdSuccess(_, _, EtcdValueNode(_, value, _, _, _, _), _) => value
    case EtcdSuccess(_, _, EtcdDirNode(key, _, _, _, _, _), _) => throw new RuntimeException(s"$key is a directory")
    case etcdError => throw new RuntimeException(s"Etcd request failed: $etcdError")
  }
}
