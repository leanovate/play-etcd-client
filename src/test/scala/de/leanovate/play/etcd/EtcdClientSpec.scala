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

    val EtcdSuccess(etcdIndex1, action1, node1, prevNode1) = await(etcdClient.getNode("/foo"))

    etcdIndex1 mustEqual 10
    action1 mustEqual "get"
    node1 mustEqual EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(9),
      modifiedIndex = Some(10),
      expiration = Some(Instant.parse("2015-10-17T12:16:55.450716163Z")),
      ttl = Some(25)
    )

    val EtcdSuccess(etcdIndex2, action2, node2, prevNode2) = await(etcdClient.getNode("/foo",
      wait = Some(true), waitIndex = Some(10), recursive = Some(true), sorted = Some(true)))

    etcdIndex2 mustEqual 10
    action2 mustEqual "get"
    node2 mustEqual EtcdValueNode(
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

    val EtcdSuccess(etcdIndex1, action1, node1, prevNode1) = await(etcdClient.deleteValue("/foo"))

    etcdIndex1 mustEqual 10
    node1 mustEqual EtcdValueNode(
      key = "/foo",
      value = "",
      createdIndex = Some(6),
      modifiedIndex = Some(7),
      expiration = None,
      ttl = None
    )
    prevNode1 mustEqual Some(EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(6),
      modifiedIndex = Some(6),
      expiration = None,
      ttl = None
    ))

    val EtcdSuccess(etcdIndex2, action2, node2, prevNode2) = await(etcdClient.deleteValue("/foo",
      prevIndex = Some(10), prevValue = Some("bla")))

    etcdIndex2 mustEqual 10
    node2 mustEqual EtcdValueNode(
      key = "/foo",
      value = "",
      createdIndex = Some(6),
      modifiedIndex = Some(7),
      expiration = None,
      ttl = None
    )
    prevNode2 mustEqual Some(EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(6),
      modifiedIndex = Some(6),
      expiration = None,
      ttl = None
    ))
  }

  it should "delete dir nodes" in new WithMocks {
    override def etcdRoute: Route = Route {
      case (DELETE, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(
          Json.obj(
            "action" -> "delete",
            "node" -> Json.obj(
              "key" -> "/foo",
              "dir" -> true,
              "modifiedIndex" -> 7,
              "createdIndex" -> 6
            ),
            "prevNode" -> Json.obj(
              "key" -> "/foo",
              "dir" -> true,
              "modifiedIndex" -> 6,
              "createdIndex" -> 6
            )
          )
        ).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex, action, node, prevNode) = await(etcdClient.deleteDir("/foo"))

    etcdIndex mustEqual 10
    node mustEqual EtcdDirNode(
      key = "/foo",
      nodes = Seq(),
      createdIndex = Some(6),
      modifiedIndex = Some(7),
      expiration = None,
      ttl = None
    )
    prevNode mustEqual Some(EtcdDirNode(
      key = "/foo",
      nodes = Seq(),
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

    val EtcdSuccess(etcdIndex1, action1, node1, prevNode1) = await(etcdClient.updateValue("/foo", "bar"))

    etcdIndex1 mustEqual 10
    node1 mustEqual EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )

    val EtcdSuccess(etcdIndex2, action2, node2, prevNode2) = await(etcdClient.updateValue("/foo", "bar",
      prevExist = Some(true), prevIndex = Some(10), prevValue = Some("bla"), ttl = Some(10)))

    etcdIndex2 mustEqual 10
    node2 mustEqual EtcdValueNode(
      key = "/foo",
      value = "bar",
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )
  }

  it should "update dir nodes" in new WithMocks {
    override def etcdRoute: Route = Route {
      case (PUT, "http://localhost:2379/v2/keys/foo") => Action {
        Ok(
          Json.obj(
            "action" -> "set",
            "node" -> Json.obj(
              "key" -> "/foo",
              "dir" -> true,
              "modifiedIndex" -> 8,
              "createdIndex" -> 8
            )
          )
        ).withHeaders("X-Etcd-Index" -> "10")
      }
    }

    val EtcdSuccess(etcdIndex1, action1, node1, prevNode1) = await(etcdClient.updateDir("/foo"))

    etcdIndex1 mustEqual 10
    node1 mustEqual EtcdDirNode(
      key = "/foo",
      nodes = Seq(),
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )

    val EtcdSuccess(etcdIndex2, action2, node2, prevNode2) = await(etcdClient.updateDir("/foo", ttl = Some(10)))

    etcdIndex2 mustEqual 10
    node2 mustEqual EtcdDirNode(
      key = "/foo",
      nodes = Seq(),
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )
  }

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

    val EtcdSuccess(etcdIndex1, action1, node1, prevNode1) = await(etcdClient.createValue("/foo", "bar"))

    etcdIndex1 mustEqual 10
    node1 mustEqual EtcdValueNode(
      key = "/foo/12",
      value = "bar",
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )

    val EtcdSuccess(etcdIndex2, action12, node2, prevNode2) = await(etcdClient.createValue("/foo", "bar", ttl = Some(10)))

    etcdIndex2 mustEqual 10
    node2 mustEqual EtcdValueNode(
      key = "/foo/12",
      value = "bar",
      createdIndex = Some(8),
      modifiedIndex = Some(8),
      expiration = None,
      ttl = None
    )
  }

  it should "fail if etcd-index header is missing" in new WithMocks {
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
        ))
      }
    }

    intercept[RuntimeException] {
      await(etcdClient.getNode("/foo"))
    }
  }

  trait WithMocks {
    def etcdRoute: Route

    val mockWS = MockWS(etcdRoute)
    val etcdClient = new EtcdClient("http://localhost:2379", mockWS)
  }
}

