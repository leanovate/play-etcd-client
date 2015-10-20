package de.leanovate.play.etcd

import java.net.InetAddress
import javax.inject.Inject

import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.Future
import scala.util.Try

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
  def getValues(key: String): Future[Seq[String]] = etcdClient.getNode(key).map {
    case EtcdSuccess(_, _, EtcdValueNode(_, value, _, _, _, _), _) =>
      Seq(value)
    case EtcdSuccess(_, _, EtcdDirNode(_, nodes, _, _, _, _), _) =>
      nodes.flatMap {
        case EtcdValueNode(_, value, _, _, _, _) => Seq(value)
        case _ => Seq()
      }
    case _ => Seq()
  }

  /**
   * Tries an atomic transformation of a value node.
   *
   * The transformation is only tried once and might fail.
   *
   * @param key The key of the node to transform
   * @param transformation The transformation
   * @return The current value of the and if the transformation was successful
   */
  def tryTransformValue(key: String, transformation: (String) => String): Future[(Boolean, String)] = {
    etcdClient.getNode(key).map(valueFromResult).flatMap {
      value =>
        etcdClient.updateValue(key, transformation(value), prevValue = Some(value)).flatMap {
          case EtcdSuccess(_, _, EtcdValueNode(_, newValue, _, _, _, _), _) =>
            Future.successful((true, newValue))
          case EtcdError(_, _, EtcdErrorCodes.COMPARE_FAILED, _, _) =>
            etcdClient.getNode(key).map(valueFromResult).map((false, _))
          case etcdError =>
            throw new RuntimeException(s"Etcd request failed: $etcdError")
        }
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
   * @param value the value to enqueue
   * @param ttl Optional time to live of the value
   * @return the key of the enqueued node
   */
  def enqueueValue(dirKey: String, value: String, ttl: Option[Long] = None): Future[String] =
    etcdClient.createValue(dirKey, value, ttl).map {
      case EtcdSuccess(_, _, EtcdValueNode(key, _, _, _, _, _), _) => key
      case etcdError => throw new RuntimeException(s"Etcd request failed: $etcdError")
    }

  /**
   * Dequeue a value when (mis)using etcd as queueing service.
   *
   * The queue is supposed to behave like a fifo and ony once client is supposed to receive the value.
   *
   * @param dirKey the directory node to use for queuing
   * @return dequed value
   */
  def dequeueValue(dirKey: String): Future[String] = {
    def tryPop(etcdIndex: Long, nodes: Seq[EtcdValueNode]): Future[String] =
      nodes.headOption.map {
        node =>
          etcdClient.deleteValue(node.key, prevIndex = node.modifiedIndex).flatMap {
            case _: EtcdSuccess =>
              Future.successful(node.value)
            case EtcdError(_, _, EtcdErrorCodes.COMPARE_FAILED, _, _) | EtcdError(_, _, EtcdErrorCodes.KEY_NOT_FOUND, _, _) =>
              tryPop(etcdIndex, nodes.tail)
            case etcdError =>
              throw new RuntimeException(s"Etcd request failed: $etcdError")
          }
      }.getOrElse {
        waitCandidates(etcdIndex + 1)
      }

    def tryPopCandidates(): Future[String] =
      etcdClient.getNode(dirKey).map(nodesFromResult).flatMap {
        case (etcdIndex, nodes) =>
          tryPop(etcdIndex,
            nodes.flatMap {
              case valueNode: EtcdValueNode => Seq(valueNode)
              case _ => Seq.empty
            })
      }

    def waitCandidates(waitIndex: Long): Future[String] = {
      etcdClient.getNode(dirKey, sorted = Some(true), waitIndex = Some(waitIndex),
        wait = Some(true), recursive = Some(true)).flatMap {
        case EtcdSuccess(_, _, _, _) => tryPopCandidates()
        case etcdError => Future.failed(new RuntimeException(s"Etcd request failed: $etcdError"))
      }
    }

    tryPopCandidates()
  }

  /**
   * Try to run a code block with a lock.
   *
   * I.e. only one client is supposed to run a `block` at a time. If another client is currently holding the lock,
   * the `block` is not executed.
   *
   * @param key The key to use for locking (will become a value key)
   * @param ttl Optional time to live of the key (recommended to prevent deadlocks in case of a major failure)
   * @param block The code block requiring cluster-wide synchronization
   * @return etcd index of the try, result of the block
   */
  def tryLock[T](key: String, ttl: Option[Long])(block: => T): Future[(Long, Option[Try[T]])] = {
    etcdClient.updateValue(key, InetAddress.getLocalHost.getHostName,
      ttl = ttl, prevExist = Some(false)).flatMap {
      case _: EtcdSuccess =>
        val result = Try(block)
        etcdClient.deleteValue(key).map {
          case EtcdSuccess(etcdIndex, _, _, _) => (etcdIndex, Some(result))
          case etcdError => throw new RuntimeException(s"Etcd request failed: $etcdError")
        }
      case EtcdError(etcdIndex, _, EtcdErrorCodes.KEY_ALREADY_EXISTS, _, _) =>
        Future.successful((etcdIndex, None))
      case etcdError => throw new RuntimeException(s"Etcd request failed: $etcdError")
    }
  }

  /**
   * Lock a code block via etcd.
   *
   * I.e. only one client is supposed to run a `block` at a time.
   *
   * @param key The key to use for locking (will become a value key)
   * @param ttl Optional time to live of the key (recommended to prevent deadlocks in case of a major failure)
   * @param block The code block requiring cluster-wide synchronization
   * @return result of the block
   */
  def lock[T](key: String, ttl: Option[Long] = None)(block: => T): Future[Try[T]] = {
    tryLock(key, ttl)(block).flatMap {
      case (_, Some(result)) => Future.successful(result)
      case (etcdIndex, None) => etcdClient.getNode(key, wait = Some(true), waitIndex = Some(etcdIndex + 1)).flatMap {
        _ =>
          lock(key, ttl)(block)
      }
    }
  }

  private def valueFromResult(result: EtcdResult): String = result match {
    case EtcdSuccess(_, _, EtcdValueNode(_, value, _, _, _, _), _) => value
    case EtcdSuccess(_, _, EtcdDirNode(key, _, _, _, _, _), _) => throw new RuntimeException(s"$key is a directory")
    case etcdError => throw new RuntimeException(s"Etcd request failed: $etcdError")
  }

  private def nodesFromResult(result: EtcdResult): (Long, Seq[EtcdNode]) = result match {
    case EtcdSuccess(etcdIndex, _, EtcdDirNode(_, nodes, _, _, _, _), _) => (etcdIndex, nodes)
    case EtcdSuccess(_, _, EtcdValueNode(key, _, _, _, _, _), _) => throw new RuntimeException(s"$key is not a directory")
    case etcdError => throw new RuntimeException(s"Etcd request failed: $etcdError")
  }
}
