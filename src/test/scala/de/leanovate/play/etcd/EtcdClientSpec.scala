package de.leanovate.play.etcd

import java.time.Instant

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
            "createdIndex" -> 9,
            "expiration" -> "2015-10-17T12:16:55.450716163Z",
            "ttl" -> 25
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
      expiration = Some(Instant.parse("2015-10-17T12:16:55.450716163Z")),
      ttl = Some(25)
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
    action mustEqual "get"
    node mustEqual EtcdDirNode("/foo",
      Seq(
        EtcdValueNode("/foo/bar", "foobar", Some(9), Some(10), None, None),
        EtcdDirNode("/foo/foo", Seq(), Some(9), Some(10), None, None)
      ), Some(9), Some(10), None, None)
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

  it should "delete value nodes" in new WithMocks {
    override def etcdRoute: Route = Route {
      case (DELETE, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(
          Json.obj(
            "action" -> "delete",
            "node" -> Json.obj(
              "key" -> "/foo",
              "modifiedIndex" -> 7,
              "createdIndex" -> 6
            ),
            "prevNode" -> Json.obj(
              "key" -> "/foo",
              "value" -> "bar",
              "modifiedIndex" -> 6,
              "createdIndex" -> 6
            )
          )
        ).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex, action, node, prevNode) = await(etcdClient.deleteValue("/foo"))

    etcdIndex mustEqual 10
    node mustEqual EtcdValueNode(
      key = "/foo",
      value = "",
      createdIndex = Some(6),
      modifiedIndex = Some(7),
      expiration = None,
      ttl = None
    )
    prevNode mustEqual Some(EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(6),
      modifiedIndex = Some(6),
      expiration = None,
      ttl = None
    ))
  }

  it should "update value nodes" in new WithMocks {
    override def etcdRoute: Route = Route {
      case (PUT, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(
          Json.obj(
            "action" -> "set",
            "node" -> Json.obj(
              "key" -> "/foo",
              "value" -> "bar",
              "modifiedIndex" -> 8,
              "createdIndex" -> 8
            )
          )
        ).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex, action, node, prevNode) = await(etcdClient.updateValue("/foo", "bar"))

    etcdIndex mustEqual 10
    node mustEqual EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )
  }

  // {"action":"create","node":{"key":"/har/12","value":"bla","modifiedIndex":12,"createdIndex":12}}

  it should "create value nodes in directory" in new WithMocks {
    override def etcdRoute: Route = Route {
      case (POST, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(
          Json.obj(
            "action" -> "create",
            "node" -> Json.obj(
              "key" -> "/foo/12",
              "value" -> "bar",
              "modifiedIndex" -> 8,
              "createdIndex" -> 8
            )
          )
        ).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex, action, node, prevNode) = await(etcdClient.createValue("/foo", "bar"))

    etcdIndex mustEqual 10
    node mustEqual EtcdValueNode(
      key = "/foo/12",
      value = "bar",
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )
  }

  trait WithMocks {
    def etcdRoute: Route

    val mockWS = MockWS(etcdRoute)
    val etcdClient = new EtcdClient("http://localhost:2379", mockWS)
  }

}

