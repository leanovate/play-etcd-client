package de.leanovate.play.etcd

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.mockito.Matchers.{eq => eql, _}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, MustMatchers}
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.Future
import scala.util.Success

class EtcdOperationsSpec extends FlatSpec with MustMatchers with MockitoSugar with FutureAwaits with DefaultAwaitTimeout {

  it should "get value" in new WithMocks {
    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdError(12, testKey, EtcdErrorCodes.KEY_NOT_FOUND, 12, "Not found")))

    await(etcdOperations.getValues(testKey)) mustEqual Seq()

    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdSuccess(12, "get", EtcdValueNode(testKey, "some value", None, None, None, None), None)))

    await(etcdOperations.getValues(testKey)) mustEqual Seq("some value")

    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(
      EtcdSuccess(12, "get",
        EtcdDirNode(testKey, Seq(
          EtcdValueNode(testKey + "/1", "first", None, None, None, None),
          EtcdDirNode(testKey + "/2", Seq(), None, None, None, None),
          EtcdValueNode(testKey + "/3", "second", None, None, None, None)
        ), None, None, None, None), None)))

    await(etcdOperations.getValues(testKey)) mustEqual Seq("first", "second")
  }

  it should "successfully try atomic transformation" in new WithMocks {
    val mockTransform = mock[(String) => String]

    when(mockTransform.apply(anyString())).thenReturn("new value")
    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdSuccess(12, "get", EtcdValueNode(testKey, "old value", None, None, None, None), None)))
    when(mockEtcdClient.updateValue(testKey, "new value", prevValue = Some("old value")))
      .thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "new value", None, None, None, None), None)))

    val result = await(etcdOperations.tryTransformValue(testKey, mockTransform))

    result mustEqual(true, "new value")

    verify(mockTransform).apply("old value")
    verifyNoMoreInteractions(mockTransform)
  }

  it should "unsuccessfully try atomic transformation" in new WithMocks {
    val mockTransform = mock[(String) => String]

    when(mockTransform.apply(anyString())).thenReturn("new value")
    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdSuccess(12, "get", EtcdValueNode(testKey, "old value", None, None, None, None), None)))
    when(mockEtcdClient.updateValue(testKey, "new value", prevValue = Some("old value")))
      .thenReturn(Future.successful(EtcdError(12, testKey, EtcdErrorCodes.COMPARE_FAILED, 12, "compare failed")))

    val result = await(etcdOperations.tryTransformValue(testKey, mockTransform))

    result mustEqual(false, "old value")

    verify(mockTransform).apply("old value")
    verifyNoMoreInteractions(mockTransform)
  }

  it should "retry failed atomic transformations" in new WithMocks {
    val mockTransform = mock[(String) => String]

    when(mockTransform.apply(anyString())).thenReturn("new value")
    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdSuccess(12, "get", EtcdValueNode(testKey, "old value", None, None, None, None), None)))
      .thenReturn(Future.successful(EtcdSuccess(13, "get", EtcdValueNode(testKey, "old value2", None, None, None, None), None)))
    when(mockEtcdClient.updateValue(testKey, "new value", prevValue = Some("old value")))
      .thenReturn(Future.successful(EtcdError(12, testKey, EtcdErrorCodes.COMPARE_FAILED, 12, "compare failed")))
    when(mockEtcdClient.updateValue(testKey, "new value", prevValue = Some("old value2")))
      .thenReturn(Future.successful(EtcdSuccess(14, "set", EtcdValueNode(testKey, "new value", None, None, None, None), None)))

    val result = await(etcdOperations.transformValue(testKey, mockTransform))

    result mustEqual "new value"

    verify(mockTransform).apply("old value")
    verify(mockTransform).apply("old value2")
    verifyNoMoreInteractions(mockTransform)
  }

  it should "enqueue value" in new WithMocks {
    when(mockEtcdClient.createValue(testKey, "value", None))
      .thenReturn(Future.successful(EtcdSuccess(12, "create", EtcdValueNode(testKey + "/12", "value", None, None, None, None), None)))

    val result = await(etcdOperations.enqueueValue(testKey, "value", None))

    result mustEqual testKey + "/12"
  }

  it should "dequeue value" in new WithMocks {
    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdSuccess(12, "get",
      EtcdDirNode(testKey, Seq(
        EtcdValueNode(testKey + "/10", "value1", None, Some(10), None, None),
        EtcdValueNode(testKey + "/11", "value2", None, Some(11), None, None)
      ), None, None, None, None), None)))
    when(mockEtcdClient.deleteValue(testKey + "/10", prevIndex = Some(10)))
      .thenReturn(Future.successful(
      EtcdError(12, testKey + "/10", EtcdErrorCodes.COMPARE_FAILED, 12, "")
    ))
    when(mockEtcdClient.deleteValue(testKey + "/11", prevIndex = Some(11)))
      .thenReturn(Future.successful(
      EtcdSuccess(12, "delete",
        EtcdValueNode(testKey + "/11", "", None, Some(11), None, None),
        Some(EtcdValueNode(testKey + "/11", "value2", None, Some(11), None, None)))
    ))

    val result = await(etcdOperations.dequeueValue(testKey))

    result mustEqual "value2"
  }

  it should "try lock success" in new WithMocks {
    when(mockEtcdClient.updateValue(any[String], any[String], any[Option[Long]], any[Option[String]], any[Option[Long]], any[Option[Boolean]])).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))
    when(mockEtcdClient.deleteValue(testKey)).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))

    val ran = new AtomicBoolean(false)
    val result = await(etcdOperations.tryLock(testKey, ttl = Some(30)) {
      ran.set(true)
      "the result"
    })

    ran.get() mustEqual true
    result._1 mustEqual 12
    result._2 mustEqual Some(Success("the result"))

    verify(mockEtcdClient).updateValue(eql(testKey), any[String], eql(Some(30)), any[Option[String]], any[Option[Long]], eql(Some(false)))
    verify(mockEtcdClient, times(2)).deleteValue(testKey)
  }

  it should "try lock failure" in new WithMocks {
    when(mockEtcdClient.updateValue(any[String], any[String], any[Option[Long]], any[Option[String]], any[Option[Long]], any[Option[Boolean]])).
      thenReturn(Future.successful(EtcdError(12, testKey, EtcdErrorCodes.KEY_ALREADY_EXISTS, 12, "")))

    val ran = new AtomicBoolean(false)
    val result = await(etcdOperations.tryLock(testKey, ttl = Some(30)) {
      ran.set(true)
      "the result"
    })

    ran.get() mustEqual false
    result._1 mustEqual 12
    result._2 mustEqual None

    verify(mockEtcdClient).updateValue(eql(testKey), any[String], eql(Some(30)), any[Option[String]], any[Option[Long]], eql(Some(false)))
  }

  it should "lock success" in new WithMocks {
    when(mockEtcdClient.updateValue(any[String], any[String], any[Option[Long]], any[Option[String]], any[Option[Long]], any[Option[Boolean]])).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))
    when(mockEtcdClient.deleteValue(testKey)).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))

    val ran = new AtomicInteger(0)
    val result = await(etcdOperations.lock(testKey, ttl = Some(30)) {
      ran.incrementAndGet()
      "the result"
    })

    ran.get() mustEqual 1
    result mustEqual Success("the result")

    verify(mockEtcdClient).updateValue(eql(testKey), any[String], eql(Some(30)), any[Option[String]], any[Option[Long]], eql(Some(false)))
    verify(mockEtcdClient, times(2)).deleteValue(testKey)
  }

  it should "lock success retry" in new WithMocks {
    when(mockEtcdClient.updateValue(any[String], any[String], any[Option[Long]], any[Option[String]], any[Option[Long]], any[Option[Boolean]])).
      thenReturn(Future.successful(EtcdError(12, testKey, EtcdErrorCodes.KEY_ALREADY_EXISTS, 12, ""))).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))
    when(mockEtcdClient.deleteValue(testKey)).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))
    when(mockEtcdClient.getNode(any[String], any[Option[Boolean]], any[Option[Boolean]], any[Option[Boolean]], any[Option[Long]])).
      thenReturn(Future.successful(EtcdSuccess(12, "set", EtcdValueNode(testKey, "", None, Some(11), None, None), None)))

    val ran = new AtomicInteger(0)
    val result = await(etcdOperations.lock(testKey, ttl = Some(30)) {
      ran.incrementAndGet()
      "the result"
    })

    ran.get() mustEqual 1
    result mustEqual Success("the result")

    verify(mockEtcdClient, times(2)).updateValue(eql(testKey), any[String], eql(Some(30)), any[Option[String]], any[Option[Long]], eql(Some(false)))
    verify(mockEtcdClient, times(2)).deleteValue(testKey)
    verify(mockEtcdClient).getNode(eql(testKey), eql(Some(true)), any[Option[Boolean]], any[Option[Boolean]], eql(Some(13)))
  }

  trait WithMocks {
    val mockEtcdClient = mock[EtcdClient]

    val etcdOperations = new EtcdOperations(mockEtcdClient)

    val testKey = "some key"
  }
}
