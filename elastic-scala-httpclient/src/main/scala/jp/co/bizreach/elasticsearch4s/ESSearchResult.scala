package jp.co.bizreach.elasticsearch4s

case class ESSearchResultTotal(value: Int, relation: String)

object ESSearchResultTotal {
  def apply(map: Map[String, Any]): ESSearchResultTotal = {
    ESSearchResultTotal(map("value").asInstanceOf[Int], map("relation").asInstanceOf[String])
  }
}

case class ESSearchResult[T](
  total: ESSearchResultTotal,
  totalHits: Long,
  tookTime: Long,
  list: List[ESSearchResultItem[T]],
  facets: Map[String, Map[String, Any]],
  aggregations: Map[String, Any],
  source: Map[String, Any]
)

case class ESSearchResultItem[T](
  id: String,
  score: Double,
  doc: T,
  highlightFields: Map[String, List[String]],
  explanation: Map[String, Any],
  sort: Seq[Any]
)
