package jp.co.bizreach.elasticsearch4s.retry

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.FunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class RetryTest extends FunSuite {

  test("retryBlocking (success)"){
    implicit val retryConfig = RetryConfig(2, 1 second, FixedBackOff)
    var count = 0

    val result = retryBlocking {
      count = count + 1
      if(count < 3){
        throw new RuntimeException()
      }
      count
    }

    assert(result == 3)
  }

  test("retryBlocking (failure)"){
    implicit val retryConfig = RetryConfig(2, 1 second, FixedBackOff)
    var count = 0

    assertThrows[RuntimeException] {
      retryBlocking {
        count = count + 1
        if(count < 4){
          throw new RuntimeException()
        }
        count
      }
    }
  }

  test("retryFuture (success)"){
    implicit val retryConfig = RetryConfig(2, 1 second, FixedBackOff)
    implicit val retryManager = new FutureRetryManager()
    var count = 0

    val f = retryFuture {
      Future {
        count = count + 1
        if(count < 3){
          throw new RuntimeException()
        }
        count
      }
    }
    val result = Await.result(f, 5 seconds)

    assert(result == 3)
    retryManager.shutdown()
  }

  test("retryFuture (failure)"){
    implicit val retryConfig = RetryConfig(2, 1 second, FixedBackOff)
    implicit val retryManager = new FutureRetryManager()
    var count = 0

    val f = retryFuture {
      Future {
        count = count + 1
        if(count < 4){
          throw new RuntimeException()
        }
        count
      }
    }

    assertThrows[RuntimeException]{
      Await.result(f, 5 seconds)
    }

    retryManager.shutdown()
  }

  test("BackOff is serializable"){
    {
      val out = new ByteArrayOutputStream()
      new ObjectOutputStream(out).writeObject(LinerBackOff)

      val in = new ByteArrayInputStream(out.toByteArray)
      val backOff = new ObjectInputStream(in).readObject()

      assert(backOff == LinerBackOff)
    }
    {
      val out = new ByteArrayOutputStream()
      new ObjectOutputStream(out).writeObject(ExponentialBackOff)

      val in = new ByteArrayInputStream(out.toByteArray)
      val backOff = new ObjectInputStream(in).readObject()

      assert(backOff == ExponentialBackOff)
    }
    {
      val out = new ByteArrayOutputStream()
      new ObjectOutputStream(out).writeObject(FixedBackOff)

      val in = new ByteArrayInputStream(out.toByteArray)
      val backOff = new ObjectInputStream(in).readObject()

      assert(backOff == FixedBackOff)
    }
  }

}
