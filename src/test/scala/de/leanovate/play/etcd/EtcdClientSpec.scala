package de.leanovate.play.etcd

import mockws.{MockWS, Route}
import org.scalatest.{FlatSpec, MustMatchers}
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.Results._
import play.api.test.Helpers._
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

class EtcdClientSpec extends FlatSpec with MustMatchers with FutureAwaits with DefaultAwaitTimeout {
  it should "get value nodes" in new WithMocks {
    override def etcdRoute = Route {
      case (GET, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(Json.obj(
          "action" -> "get",
          "node" -> Json.obj(
            "key" -> "/foo",
            "value" -> "bar",
            "modifiedIndex" -> 10,
            "createdIndex" -> 9
          )
        )).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex, action, node, prevNode) = await(etcdClient.getNode("/foo"))

    etcdIndex mustEqual 10
    action mustEqual "get"
    node mustEqual EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(9),
      modifiedIndex = Some(10),
      expiration = None,
      ttl = None
    )
  }

  it should "get directory nodes" in new WithMocks {
    override def etcdRoute = Route {
      case (GET, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(Json.obj(
          "action" -> "get",
          "node" -> Json.obj(
            "key" -> "/foo",
            "dir" -> true,
            "nodes" -> Json.arr(
              Json.obj(
                "key" -> "/foo/bar",
                "value" -> "foobar",
                "modifiedIndex" -> 10,
                "createdIndex" -> 9
              ),
              Json.obj(
                "key" -> "/foo/foo",
                "dir" -> true,
                "modifiedIndex" -> 10,
                "createdIndex" -> 9
              )
            ),
            "modifiedIndex" -> 10,
            "createdIndex" -> 9
          )
        )).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex, action, node, prevNode) = await(etcdClient.getNode("/foo"))

    etcdIndex mustEqual 10
  }

  it should "handle missing nodes" in new WithMocks {
    override def etcdRoute: Route = Route {
      case (GET, "http://localhost:2379/v2/keys/foo") => Action {
        NotFound(
          Json.obj(
            "errorCode" -> 100,
            "message" -> "Key not found",
            "cause" -> "/foo",
            "index" -> 10
          )).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdError(etcdIndex, cause, errorCode, index, message) = await(etcdClient.getNode("/foo"))

    etcdIndex mustEqual 10
    cause mustEqual "/foo"
    message mustEqual "Key not found"
    errorCode mustEqual EtcdErrorCodes.KEY_NOT_FOUND
    index mustEqual 10
  }

  trait WithMocks {
    def etcdRoute: Route

    val mockWS = MockWS(etcdRoute)
    val etcdClient = new EtcdClient("http://localhost:2379", mockWS)
  }

}

