package jp.co.bizreach

import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.geo.builders.ShapeBuilder
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilder}

import scala.reflect.ClassTag

package object elasticsearch4s {

  def string2config(indexName: String) = ESConfig(indexName)
  def tuple2config(tuple: (String, String)) = ESConfig(tuple._1, tuple._2)

  implicit class ESStringConfig(indexName: String){
    def / (typeName: String): ESConfig = ESConfig(indexName, typeName)
  }

  def matchAllQuery = QueryBuilders.matchAllQuery
  def matchQuery(name: String, text: AnyRef) = QueryBuilders.matchQuery(name, text)
  def multiMatchQuery(text: AnyRef, fieldNames: String*) = QueryBuilders.multiMatchQuery(text, fieldNames: _*)
  def matchPhraseQuery(name: String, text: AnyRef) = QueryBuilders.matchPhraseQuery(name, text)
  def matchPhrasePrefixQuery(name: String, text: AnyRef) = QueryBuilders.matchPhrasePrefixQuery(name, text)
  def disMaxQuery = QueryBuilders.disMaxQuery
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
  def spanMultiTermQueryBuilder(multiTermQueryBuilder: MultiTermQueryBuilder) = QueryBuilders.spanMultiTermQueryBuilder(multiTermQueryBuilder)
  def fieldMaskingSpanQuery(query: SpanQueryBuilder, field: String) = QueryBuilders.fieldMaskingSpanQuery(query, field)
  def constantScoreQuery(queryBuilder: QueryBuilder) = QueryBuilders.constantScoreQuery(queryBuilder)
  def functionScoreQuery(queryBuilder: QueryBuilder) = QueryBuilders.functionScoreQuery(queryBuilder)
  def functionScoreQuery(queryBuilder: QueryBuilder, filterFunctionBuilders: Array[FunctionScoreQueryBuilder.FilterFunctionBuilder]) = QueryBuilders.functionScoreQuery(queryBuilder, filterFunctionBuilders)
  def functionScoreQuery[T <: ScoreFunctionBuilder[T]](function: ScoreFunctionBuilder[T]) = QueryBuilders.functionScoreQuery(function)
  def functionScoreQuery[T <: ScoreFunctionBuilder[T]](queryBuilder: QueryBuilder, function: ScoreFunctionBuilder[T]) = QueryBuilders.functionScoreQuery(queryBuilder, function)
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
  def geoShapeQuery(name: String, shape: ShapeBuilder) = QueryBuilders.geoShapeQuery(name, shape)
  def geoShapeQuery(name: String, indexedShapeId: String, indexedShapeType: String) = QueryBuilders.geoShapeQuery(name, indexedShapeId, indexedShapeType)

}

