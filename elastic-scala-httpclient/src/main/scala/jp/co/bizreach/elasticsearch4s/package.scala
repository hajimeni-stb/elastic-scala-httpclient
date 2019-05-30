package jp.co.bizreach

import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.geo.GeoPoint
import org.elasticsearch.index.query.{MoreLikeThisQueryBuilder, MultiTermQueryBuilder, QueryBuilder, QueryBuilders, SpanQueryBuilder}
import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilder}
import org.elasticsearch.indices.TermsLookup

import scala.reflect.ClassTag

package object elasticsearch4s {

  def string2config(indexName: String) = ESConfig(indexName)

  implicit class ESStringConfig(indexName: String){
    @deprecated("Types will be deprecated in APIs in Elasticsearch 7.0.0, and completely removed in 8.0.0.", "4.0.0")
    def / (typeName: String): ESConfig = ESConfig(indexName, Some(typeName))
  }

  def matchAllQuery = QueryBuilders.matchAllQuery
  def matchQuery(name: String, text: AnyRef) = QueryBuilders.matchQuery(name, text)
  def commonTermsQuery(name: String, text: AnyRef) = QueryBuilders.commonTermsQuery(name, text)
  def multiMatchQuery(text: AnyRef, fieldNames: String*) = QueryBuilders.multiMatchQuery(text, fieldNames: _*)
  def matchPhraseQuery(name: String, text: AnyRef) = QueryBuilders.matchPhraseQuery(name, text)
  def matchPhrasePrefixQuery(name: String, text: AnyRef) = QueryBuilders.matchPhrasePrefixQuery(name, text)
  def disMaxQuery = QueryBuilders.disMaxQuery
  def idsQuery() = QueryBuilders.idsQuery()
  def idsQuery(types: String*) = QueryBuilders.idsQuery(types: _*)
  def termQuery(name: String, value: String) = QueryBuilders.termQuery(name, value)
  def termQuery(name: String, value: Int) = QueryBuilders.termQuery(name, value)
  def termQuery(name: String, value: Long) = QueryBuilders.termQuery(name, value)
  def termQuery(name: String, value: Float) = QueryBuilders.termQuery(name, value)
  def termQuery(name: String, value: Double) = QueryBuilders.termQuery(name, value)
  def termQuery(name: String, value: Boolean) = QueryBuilders.termQuery(name, value)
  def termQuery(name: String, value: AnyRef) = QueryBuilders.termQuery(name, value)
  def fuzzyQuery(name: String, value: String) = QueryBuilders.fuzzyQuery(name, value)
  def fuzzyQuery(name: String, value: AnyRef) = QueryBuilders.fuzzyQuery(name, value)
  def prefixQuery(name: String, prefix: String) = QueryBuilders.prefixQuery(name, prefix)
  def rangeQuery(name: String) = QueryBuilders.rangeQuery(name)
  def wildcardQuery(name: String, query: String) = QueryBuilders.wildcardQuery(name, query)
  def regexpQuery(name: String, regexp: String) = QueryBuilders.regexpQuery(name, regexp)
  def queryStringQuery(queryString: String) = QueryBuilders.queryStringQuery(queryString)
  def simpleQueryStringQuery(queryString: String) = QueryBuilders.simpleQueryStringQuery(queryString)
  def boostingQuery(positiveQuery: QueryBuilder, negativeQuery: QueryBuilder) = QueryBuilders.boostingQuery(positiveQuery, negativeQuery)
  def boolQuery = QueryBuilders.boolQuery
  def spanTermQuery(name: String, value: String) = QueryBuilders.spanTermQuery(name, value)
  def spanTermQuery(name: String, value: Int) = QueryBuilders.spanTermQuery(name, value)
  def spanTermQuery(name: String, value: Long) = QueryBuilders.spanTermQuery(name, value)
  def spanTermQuery(name: String, value: Float) = QueryBuilders.spanTermQuery(name, value)
  def spanTermQuery(name: String, value: Double) = QueryBuilders.spanTermQuery(name, value)
  def spanFirstQuery(`match`: SpanQueryBuilder, end: Int) = QueryBuilders.spanFirstQuery(`match`, end)
  def spanNearQuery(initialClause: SpanQueryBuilder, slop: Int) = QueryBuilders.spanNearQuery(initialClause, slop)
  def spanNotQuery(initialClause: SpanQueryBuilder, exclude: SpanQueryBuilder) = QueryBuilders.spanNotQuery(initialClause, exclude)
  def spanOrQuery(initialClause: SpanQueryBuilder) = QueryBuilders.spanOrQuery(initialClause)
  def spanWithinQuery(big: SpanQueryBuilder, little: SpanQueryBuilder) = QueryBuilders.spanWithinQuery(big, little)
  def spanContainingQuery(big: SpanQueryBuilder, little: SpanQueryBuilder) = QueryBuilders.spanContainingQuery(big, little)
  def spanMultiTermQueryBuilder(multiTermQueryBuilder: MultiTermQueryBuilder) = QueryBuilders.spanMultiTermQueryBuilder(multiTermQueryBuilder)
  def fieldMaskingSpanQuery(query: SpanQueryBuilder, field: String) = QueryBuilders.fieldMaskingSpanQuery(query, field)
  def constantScoreQuery(queryBuilder: QueryBuilder) = QueryBuilders.constantScoreQuery(queryBuilder)
  def functionScoreQuery(queryBuilder: QueryBuilder) = QueryBuilders.functionScoreQuery(queryBuilder)
  def functionScoreQuery(queryBuilder: QueryBuilder, filterFunctionBuilders: Array[FunctionScoreQueryBuilder.FilterFunctionBuilder]) = QueryBuilders.functionScoreQuery(queryBuilder, filterFunctionBuilders)
  def functionScoreQuery(filterFunctionBuilders: Array[FunctionScoreQueryBuilder.FilterFunctionBuilder]) = QueryBuilders.functionScoreQuery(filterFunctionBuilders)
  def functionScoreQuery[T <: ScoreFunctionBuilder[T]](function: ScoreFunctionBuilder[T]) = QueryBuilders.functionScoreQuery(function)
  def functionScoreQuery[T <: ScoreFunctionBuilder[T]](queryBuilder: QueryBuilder, function: ScoreFunctionBuilder[T]) = QueryBuilders.functionScoreQuery(queryBuilder, function)
  def moreLikeThisQuery(likeTexts: Array[String], likeItems: Array[MoreLikeThisQueryBuilder.Item], fields: String*) = if (fields.isEmpty) QueryBuilders.moreLikeThisQuery(likeTexts, likeItems) else QueryBuilders.moreLikeThisQuery(fields.toArray, likeTexts, likeItems)
  def moreLikeThisQuery(likeTexts: String*) = QueryBuilders.moreLikeThisQuery(likeTexts.toArray)
  def nestedQuery(path: String, query: QueryBuilder, scoreMode: ScoreMode) = QueryBuilders.nestedQuery(path, query, scoreMode)
  def termsQuery[T](name: String, values: T*)(implicit c: ClassTag[T]) = {
    val clazz = c.runtimeClass
    if(clazz == classOf[Int]) {
      QueryBuilders.termsQuery(name, values.asInstanceOf[Seq[Int]]: _*)
    } else if(clazz == classOf[Long]){
      QueryBuilders.termsQuery(name, values.asInstanceOf[Seq[Long]]: _*)
    } else if(clazz == classOf[Float]){
      QueryBuilders.termsQuery(name, values.asInstanceOf[Seq[Float]]: _*)
    } else if(clazz == classOf[Double]){
      QueryBuilders.termsQuery(name, values.asInstanceOf[Seq[Double]]: _*)
    } else if(clazz == classOf[String]){
      QueryBuilders.termsQuery(name, values.asInstanceOf[Seq[String]]: _*)
    } else {
      QueryBuilders.termsQuery(name, values.asInstanceOf[Seq[AnyRef]]: _*)
    }
  }
  def wrapperQuery(source: String) = QueryBuilders.wrapperQuery(source)
  def wrapperQuery(source: Array[Byte]) = QueryBuilders.wrapperQuery(source)
  def wrapperQuery(source: BytesReference) = QueryBuilders.wrapperQuery(source)
  def typeQuery(`type`: String) = QueryBuilders.typeQuery(`type`)
  def termsLookupQuery(name: String, termsLookup: TermsLookup) = QueryBuilders.termsLookupQuery(name, termsLookup)
  def geoDistanceQuery(name: String) = QueryBuilders.geoDistanceQuery(name)
  def geoBoundingBoxQuery(name: String) = QueryBuilders.geoBoundingBoxQuery(name)
  def geoPolygonQuery(name: String, points: GeoPoint*) = QueryBuilders.geoPolygonQuery(name, collection.JavaConversions.seqAsJavaList(points))
  def geoShapeQuery(name: String, indexedShapeId: String, indexedShapeType: String) = QueryBuilders.geoShapeQuery(name, indexedShapeId, indexedShapeType)
  def geoIntersectionQuery(name: String, indexedShapeId: String, indexedShapeType: String) = QueryBuilders.geoIntersectionQuery(name, indexedShapeId, indexedShapeType)
  def geoWithinQuery(name: String, indexedShapeId: String, indexedShapeType: String) = QueryBuilders.geoWithinQuery(name, indexedShapeId, indexedShapeType)
  def geoDisjointQuery(name: String, indexedShapeId: String, indexedShapeType: String) = QueryBuilders.geoDisjointQuery(name, indexedShapeId, indexedShapeType)
  def existsQuery(name: String) = QueryBuilders.existsQuery(name)

}

