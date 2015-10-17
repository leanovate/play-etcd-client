package de.leanovate.play.etcd

import java.net.URI

import play.api.libs.ws.WS
import play.api.test.FakeApplication

import scala.util.Random

trait WithEtcdClient {
  val dockerHostIp = new URI(sys.env.getOrElse("DOCKER_HOST", sys.props.getOrElse("DOCKER_HOST", "tcp://localhost:2376"))).getHost

  val etcdClient = new EtcdClient(s"http://$dockerHostIp:2379", WS.client(new FakeApplication()))

  var testKey = ('/' +: Random.alphanumeric.take(20)).mkString
}
