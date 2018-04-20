package jp.co.bizreach.elasticsearch4s.retry

import org.scalatest.FunSuite
import scala.concurrent.duration._

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

  test("retryBlocking (error)"){
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
}
