package de.leanovate.play.etcd

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
          Range(0, 20).foldLeft(Future.successful(Seq.empty[String])) {
            (prevFuture, _) =>
              prevFuture.flatMap {
                prevResult =>
                  etcdOperations.transformValue(testKey, increment).map {
                    result => prevResult :+ result
                  }
              }
          }.onComplete {
            v => promise.complete(v)
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

  trait WithOperations extends WithEtcdClient {
    val etcdOperations = new EtcdOperations(etcdClient)
  }

}
