package de.leanovate.play.etcd

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, MustMatchers}
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.Future

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

    result mustEqual (true, "new value")

    verify(mockTransform).apply("old value")
  }

  it should "unsuccessfully try atomic transformation" in new WithMocks {
    val mockTransform = mock[(String) => String]

    when(mockTransform.apply(anyString())).thenReturn("new value")
    when(mockEtcdClient.getNode(testKey))
      .thenReturn(Future.successful(EtcdSuccess(12, "get", EtcdValueNode(testKey, "old value", None, None, None, None), None)))
    when(mockEtcdClient.updateValue(testKey, "new value", prevValue = Some("old value")))
      .thenReturn(Future.successful(EtcdError(12, testKey, EtcdErrorCodes.COMPARE_FAILED, 12, "compate failed")))

    val result = await(etcdOperations.tryTransformValue(testKey, mockTransform))

    result mustEqual (false, "old value")

    verify(mockTransform).apply("old value")
  }

  it should "retry failed atomic transformations" in new WithMocks {

  }


  trait WithMocks {
    val mockEtcdClient = mock[EtcdClient]

    val etcdOperations = new EtcdOperations(mockEtcdClient)

    val testKey = "some key"
  }
}
