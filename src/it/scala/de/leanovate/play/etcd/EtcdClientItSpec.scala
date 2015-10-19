package de.leanovate.play.etcd

import org.scalatest.{FlatSpec, MustMatchers}
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

class EtcdClientItSpec extends FlatSpec with MustMatchers with FutureAwaits with DefaultAwaitTimeout {

  it should "create, get, update and delete value key" in new WithEtcdClient {
    val EtcdError(initalEtcdIndex, cause, EtcdErrorCodes.KEY_NOT_FOUND, _, _) = await(etcdClient.getNode(testKey))

    cause mustEqual testKey

    val EtcdSuccess(createEtcdIndex, "set", createdNode: EtcdValueNode, None) =
      await(etcdClient.updateValue(testKey, "initialvalue", ttl = Some(30)))

    createdNode.value mustEqual "initialvalue"
    createdNode.createdIndex mustEqual Some(createEtcdIndex)
    createEtcdIndex must be > initalEtcdIndex

    val EtcdSuccess(updateEtcdIndex, "set", updatedNode: EtcdValueNode, Some(prevNode: EtcdValueNode)) =
      await(etcdClient.updateValue(testKey, "updatedValue", ttl = Some(30)))

    updatedNode.value mustEqual "updatedValue"
    updatedNode.modifiedIndex mustEqual Some(updateEtcdIndex)
    prevNode.value mustEqual "initialvalue"
    updateEtcdIndex must be > createEtcdIndex

    val EtcdSuccess(deleteEtcdIndex, "delete", deletedNode: EtcdValueNode, Some(prevDeleteNode: EtcdValueNode)) =
      await(etcdClient.deleteValue(testKey))

    deletedNode.modifiedIndex mustEqual Some(deleteEtcdIndex)
    prevDeleteNode.value mustEqual "updatedValue"
    deleteEtcdIndex must be > updateEtcdIndex
  }

  it should "create dir, unique node in dir, delete dir" in new WithEtcdClient {
    val EtcdError(initalEtcdIndex, cause, EtcdErrorCodes.KEY_NOT_FOUND, _, _) = await(etcdClient.getNode(testKey))

    cause mustEqual testKey

    val EtcdSuccess(createDirEtcdIndex, "set", createdDirNode: EtcdDirNode, None) =
      await(etcdClient.updateDir(testKey, ttl = Some(30)))

    createDirEtcdIndex must be > initalEtcdIndex
    createdDirNode.nodes must have size 0

    val EtcdSuccess(createEtcdIndex1, "create", createdNode1: EtcdValueNode, None) =
      await(etcdClient.createValue(testKey, "first", ttl = Some(30)))

    createEtcdIndex1 must be > createDirEtcdIndex
    createdNode1.key must startWith(testKey)
    createdNode1.value mustEqual "first"

    val EtcdSuccess(createEtcdIndex2, "create", createdNode2: EtcdValueNode, None) =
      await(etcdClient.createValue(testKey, "second", ttl = Some(30)))

    createEtcdIndex2 must be > createEtcdIndex1
    createdNode2.key must startWith(testKey)
    createdNode2.value mustEqual "second"

    val EtcdError(_, cause2, EtcdErrorCodes.DIRECTORY_NOT_EMPTY, _, _) =
      await(etcdClient.deleteDir(testKey))

    cause2 mustEqual testKey

    await(etcdClient.deleteValue(createdNode1.key)).isSuccess mustBe true
    await(etcdClient.deleteValue(createdNode2.key)).isSuccess mustBe true

    val EtcdSuccess(deleteEtcdIndex, "delete", deletedNode: EtcdDirNode, Some(prevDeleteNode: EtcdDirNode)) =
      await(etcdClient.deleteDir(testKey))

    deletedNode.key mustEqual testKey
    prevDeleteNode.key mustEqual testKey
    deleteEtcdIndex must be > createEtcdIndex2
  }

  it should "wait for changes" in new WithEtcdClient {
    val EtcdError(initalEtcdIndex, cause, EtcdErrorCodes.KEY_NOT_FOUND, _, _) = await(etcdClient.getNode(testKey))

    cause mustEqual testKey

    val EtcdSuccess(createEtcdIndex, "set", _, None) =
      await(etcdClient.updateValue(testKey, "initialvalue", ttl = Some(30)))

    val waitForChange = etcdClient.getNode(testKey, wait = Some(true), waitIndex = Some(createEtcdIndex + 1))

    Thread.sleep(3000)

    waitForChange.isCompleted mustBe false

    await(etcdClient.updateValue(testKey, "updatedValue", ttl = Some(30))).isSuccess mustBe true

    val EtcdSuccess(updateEtcdIndex, "set", updatedNode: EtcdValueNode, Some(prevNode: EtcdValueNode)) =
      await(waitForChange)

    updatedNode.value mustEqual "updatedValue"
    prevNode.value mustEqual "initialvalue"
  }
}
