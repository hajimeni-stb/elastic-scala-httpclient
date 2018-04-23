package jp.co.bizreach.elasticsearch4s

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

package object retry {

  def retryBlocking[T](f: => T)(implicit config: RetryConfig): T = {
    var count = 0

    while(true){
      try {
        return f
      } catch {
        case NonFatal(e) =>
          if(count == config.maxAttempts){
            throw e
          }
          count = count + 1
          Thread.sleep(config.backOff.nextDuration(count, config.retryDuration.toMillis))
      }
    }
    ??? // never come here
  }

  def retryFuture[T](f: => Future[T])(implicit config: RetryConfig, retryManager: FutureRetryManager, ec: ExecutionContext): Future[T] = {
    val future = f
    if(config.maxAttempts > 0){
      future.recoverWith { case _ =>
        retryManager.scheduleFuture(f)
      }
    } else {
      future
    }
  }

}
