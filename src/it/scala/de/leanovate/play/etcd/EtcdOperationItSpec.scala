package de.leanovate.play.etcd

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{FlatSpec, MustMatchers}
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.{Future, Promise}
import play.api.libs.concurrent.Execution.Implicits._

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
            case e : Throwable =>
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

    results.flatten.toSet must have size(400)

    await(etcdOperations.getValues(testKey)) mustEqual Seq("400")
  }

  it should "perform lock operation sequentially" in new WithOperations {
    val sharedCounter = new AtomicInteger()

    def createRunner(idx: Int): (Future[Int], Thread) = {
      val promise = Promise[Int]()

      val runnable = new Runnable {
        override def run(): Unit = {
          await(etcdOperations.lock(testKey, ttl = Some(30)) {
            val value = sharedCounter.incrementAndGet()
            Thread.sleep(1000)
            promise.success(value)
            sharedCounter.decrementAndGet()
          })
        }
      }

      return (promise.future, new Thread(runnable, s"runner-$idx"))
    }

    val runners = Range(0, 10).map(createRunner)

    runners.foreach(_._2.start())

    val results = runners.map(runner => await(runner._1))

    results.forall(_ == 1) mustBe true
  }

  trait WithOperations extends WithEtcdClient {
    val etcdOperations = new EtcdOperations(etcdClient)
  }

}
