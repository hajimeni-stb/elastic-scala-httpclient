package jp.co.bizreach.elasticsearch4s.retry

sealed trait BackOff extends java.io.Serializable {
  def nextDuration(count: Int, duration: Long): Long
}

object LinerBackOff extends BackOff {
  override def nextDuration(count: Int, duration: Long): Long = duration * count
}

object ExponentialBackOff extends BackOff {
  override def nextDuration(count: Int, duration: Long): Long = duration ^ count
}

object FixedBackOff extends BackOff {
  override def nextDuration(count: Int, duration: Long): Long = duration
}
