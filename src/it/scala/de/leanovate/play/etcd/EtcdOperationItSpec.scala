package de.leanovate.play.etcd

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{FlatSpec, MustMatchers}
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.{Future, Promise}
import scala.util.{Success, Try}

class EtcdOperationItSpec extends FlatSpec with MustMatchers with FutureAwaits with DefaultAwaitTimeout {

  it should "support atomic increment of mulitple threads/clients" in new WithOperations {
    def increment(value: String): String = (value.toLong + 1).toString

    def createRunner(idx: Int): (Future[Seq[String]], Thread) = {
      val promise = Promise[Seq[String]]()
      val runnable = new Runnable {
        override def run(): Unit = {
          try {
            val result = Range(0, 20).map {
              _ =>
                await(etcdOperations.transformValue(testKey, increment))
            }
            promise.success(result)
          } catch {
            case e: Throwable =>
              promise.failure(e)
          }
        }
      }

      (promise.future, new Thread(runnable, s"runner-$idx"))
    }

    await(etcdClient.updateValue(testKey, "0", ttl = Some(60))).isSuccess mustEqual true

    val runners = Range(0, 20).map(createRunner)

    runners.foreach(_._2.start())

    val results = runners.map(runner => await(runner._1))

    results.flatten.toSet must have size (400)

    await(etcdOperations.getValues(testKey)) mustEqual Seq("400")
  }

  it should "perform lock operation sequentially" in new WithOperations {
    val sharedCounter = new AtomicInteger()

    def createRunner(idx: Int): (Future[Try[Int]], Thread) = {
      val promise = Promise[Try[Int]]()

      val runnable = new Runnable {
        override def run(): Unit = {
          val value = await(etcdOperations.lock(testKey, ttl = Some(30)) {
            val first = sharedCounter.incrementAndGet()
            Thread.sleep(1000)
            val second = sharedCounter.getAndDecrement()

            Math.max(first, second)
          })
          promise.success(value)
        }
      }

      return (promise.future, new Thread(runnable, s"runner-$idx"))
    }

    val runners = Range(0, 10).map(createRunner)

    runners.foreach(_._2.start())

    val results = runners.map(runner => await(runner._1))

    results.forall(_ == Success(1)) mustBe true
  }

  it should "equeue and dequeue with multiple consumers" in new WithOperations {
    def createRunner(idx: Int): (Future[Seq[String]], Thread) = {
      val promise = Promise[Seq[String]]()

      val runnable = new Runnable {
        override def run(): Unit = {
          val result = Range(0, 10).map {
            _ =>
              await(etcdOperations.dequeueValue(testKey))
          }
          promise.success(result)
        }
      }

      (promise.future, new Thread(runnable, s"runner-$idx"))
    }

    await(etcdClient.updateDir(testKey, ttl = Some(60))).isSuccess mustEqual true

    val runners = Range(0, 10).map(createRunner)

    runners.foreach(_._2.start())

    Range(0, 100).foreach {
      idx =>
        etcdOperations.enqueueValue(testKey, s"message-$idx", ttl = Some(30))
    }

    val results = runners.map(runner => await(runner._1))
  }

  trait WithOperations extends WithEtcdClient {
    val etcdOperations = new EtcdOperations(etcdClient)
  }

}
