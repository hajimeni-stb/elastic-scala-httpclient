package jp.co.bizreach.elasticsearch4s.retry

import scala.concurrent.duration.FiniteDuration

case class RetryConfig(
  maxAttempts: Int,
  retryDuration: FiniteDuration,
  backOff: BackOff
)