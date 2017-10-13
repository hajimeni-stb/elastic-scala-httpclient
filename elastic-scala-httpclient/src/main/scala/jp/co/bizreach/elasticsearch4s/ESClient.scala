package jp.co.bizreach.elasticsearch4s

import ESClient._
import ESUtils._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.annotation.tailrec
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import org.codelibs.elasticsearch.querybuilders.SearchDslBuilder

/**
 * Helper for accessing to Elasticsearch.
 */
object ESClient {

  private val logger = LoggerFactory.getLogger(classOf[ESClient])
  private var httpClient: AsyncHttpClient = null

  /**
   * This is the entry point of processing using Elasticsearch.
   */
  def using[T](url: String,
               config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder().build(),
               scriptTemplateIsAvailable: Boolean = false)(f: ESClient => T): T = {
    val httpClient = new AsyncHttpClient(config)
    val client = new ESClient(httpClient, url, scriptTemplateIsAvailable)
    try {
      f(client)
    } finally {
      httpClient.close()
    }
  }

  /**
   * Initialize AsyncHttpClient. ESClient is available by calling this method.
   */
  def init(): Unit = {
    httpClient = HttpUtils.createHttpClient()
  }

  /**
   * Return ESClient instance.
   */
  def apply(url: String, deleteByQueryIsAvailable: Boolean = false, scriptTemplateIsAvailable: Boolean = false): ESClient = {
    if(httpClient == null){
      throw new IllegalStateException("AsyncHttpClient has not been initialized. Call ESClient.init() at first.")
    }
    new ESClient(httpClient, url, scriptTemplateIsAvailable)
  }

  /**
   * Initialize AsyncHttpClient with given configuration. ESClient is available by calling this method.
   */
  def init(config: AsyncHttpClientConfig): Unit = {
    httpClient = HttpUtils.createHttpClient(config)
  }

  /**
   * Shutdown AsyncHttpClient. ESClient is disabled by calling this method.
   */
  def shutdown() = {
    httpClient.close()
    httpClient = null
  }

}

class ESClient(httpClient: AsyncHttpClient, url: String, scriptTemplateIsAvailable: Boolean = false) {

  //private val queryClient = new QueryBuilderClient()

  def insertJson(config: ESConfig, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def insertJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url) + "/" + id, json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def insert(config: ESConfig, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, JsonUtils.serialize(entity))
  }

  def insert(config: ESConfig, id: String, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, id, JsonUtils.serialize(entity))
  }

  def updateJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updateJson:\n${json}")

    val resultJson = HttpUtils.put(httpClient, config.url(url) + "/" + id, json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def update(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updateJson(config, id, JsonUtils.serialize(entity))
  }

  def updatePartiallyJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updatePartiallyJson:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url) + "/" + id + "/_update", "{\"doc\":"+ s"${json}}")
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def updatePartially(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updatePartiallyJson(config, id, JsonUtils.serialize(entity))
  }

  def delete(config: ESConfig, id: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"delete id:\n${id}")

    val resultJson = HttpUtils.delete(httpClient, config.url(url) + "/" + id)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  /**
   * Note: Need delete-by-query plugin to use this method.
   * https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/plugins-delete-by-query.html
   */
  def deleteByQuery(config: ESConfig)(f: SearchDslBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val builder = SearchDslBuilder.builder()
    f(builder)
    val json = builder.build()
    logger.debug(s"deleteByQuery:${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url) + "/_delete_by_query", json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def count(config: ESConfig)(f: SearchDslBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val builder = SearchDslBuilder.builder()
    f(builder)
    val json = builder.build()
    logger.debug(s"countRequest:${json}")

    val resultJson = HttpUtils.post(httpClient, config.preferenceUrl(url, "_count"), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def countAsInt(config: ESConfig)(f: SearchDslBuilder => Unit): Int = {
    count(config)(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => x("count").asInstanceOf[Int]
    }
  }

  private def toJson(f: SearchDslBuilder => Unit): String = {
    val builder = SearchDslBuilder.builder()
    f(builder)
    builder.build()
  }

  def search(config: ESConfig)(f: SearchDslBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val json = toJson(f)
    logger.debug(s"searchRequest:${json}")

    searchJson(config)(json)
  }

  def searchJson(config: ESConfig)(json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    logger.debug(s"searchRequest:${json}")

    val resultJson = HttpUtils.post(httpClient, config.preferenceUrl(url, "_search"), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def searchAll(config: ESConfig)(f: SearchDslBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    count(config)(f) match {
      case Left(x)  => Left(x)
      case Right(x) => {
        val total = x("count").asInstanceOf[Int]
        search(config) { builder =>
          f(builder)
          builder.from(0).size(total)
        }
      }
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def searchByTemplate(config: ESConfig)(lang: String, template: String, params: AnyRef, options: Option[String] = None): Either[Map[String, Any], Map[String, Any]] = {
    if(scriptTemplateIsAvailable) {
      logger.debug("******** ESConfig:" + config.toString)
      val json = JsonUtils.serialize(
        Map(
          "lang" -> lang,
          "file" -> template,
          "params" -> params
        )
      )
      logger.debug(s"searchRequest:${json}")

      val resultJson = HttpUtils.post(httpClient, config.urlWithParameters(url, "_search/script_template" + options.getOrElse("")), json)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def find[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): Option[(String, T)] = {
    findJson(config)(toJson(f))
  }

  def findJson[T](config: ESConfig)(json: String)(implicit c: ClassTag[T]): Option[(String, T)] = {
    searchJson(config)(json) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => {
        val hits = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]
        if(hits.length == 0){
          None
        } else {
          Some((hits.head("_id").toString, JsonUtils.deserialize[T](JsonUtils.serialize(getDocumentMap(hits.head)))))
        }
      }
    }
  }

  def findAsList[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): List[(String, T)] = {
    findAsListJson(config)(toJson(f))
  }

  def findAsListJson[T](config: ESConfig)(json: String)(implicit c: ClassTag[T]): List[(String, T)] = {
    searchJson(config)(json) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => createESSearchResult(x).list.map { x => (x.id, x.doc) }
    }
  }

  def findAllAsList[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): List[(String, T)] = {
    findAsList(config){ builder =>
      f(builder)
      builder.from(0).size(countAsInt(config)(f))
    }
  }


  def list[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    listJson(config)(toJson(f))
  }

  def listJson[T](config: ESConfig)(json: String)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    searchJson(config)(json) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => createESSearchResult(x)
    }
  }

  def listAll[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    list(config){ builder =>
      f(builder)
      builder.from(0).size(countAsInt(config)(f))
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def listByTemplate[T](config: ESConfig)(lang: String, template: String, params: AnyRef)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    if(scriptTemplateIsAvailable) {
      searchByTemplate(config)(lang, template, params) match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => createESSearchResult(x)
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def countByTemplate(config: ESConfig)(lang: String, template: String, params: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    if(scriptTemplateIsAvailable) {
      searchByTemplate(config)(lang, template, params, Some("?search_type=query_then_fetch&size=0"))
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def countByTemplateAsInt(config: ESConfig)(lang: String, template: String, params: AnyRef): Int = {
    if(scriptTemplateIsAvailable) {
      countByTemplate(config)(lang: String, template: String, params: AnyRef) match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def refresh(config: ESConfig)(): Either[Map[String, Any], Map[String, Any]] = {
    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/_refresh", "")
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
  }

  def clusterHealth(): Map[String, Any] = {
    val resultJson = HttpUtils.get(httpClient, s"${url}/_cluster/health")
    JsonUtils.deserialize[Map[String, Any]](resultJson)
  }

  def scroll[T, R](config: ESConfig)(f: SearchDslBuilder => Unit)(p: (String, T) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    logger.debug("******** ESConfig:" + config.toString)
    val builder = SearchDslBuilder.builder()
    f(builder)
    val json = builder.build()
    logger.debug(s"searchRequest:${json}")

    _scroll0(true, config.url(url) + "/_search", json, Stream.empty,
      (_id: String, map: Map[String, Any]) => p(_id, JsonUtils.deserialize[T](JsonUtils.serialize(map))))
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def scrollByTemplate[T, R](config: ESConfig)(lang: String, template: String, params: AnyRef)(p: (String, T) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    if(scriptTemplateIsAvailable) {
      logger.debug("******** ESConfig:" + config.toString)
      val json = JsonUtils.serialize(
        Map(
          "lang" -> lang,
          "file" -> template,
          "params" -> params
        )
      )
      logger.debug(s"searchRequest:${json}")

      _scroll0(true, config.url(url) + "/_search/script_template", json, Stream.empty,
        (_id: String, map: Map[String, Any]) => p(_id, JsonUtils.deserialize[T](JsonUtils.serialize(map))))

    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  @tailrec
  private def _scroll0[R](init: Boolean, searchUrl: String, body: String, stream: Stream[R], invoker: (String, Map[String, Any]) => R): Stream[R] = {
    val resultJson = if(init){
      HttpUtils.post(httpClient, searchUrl + "?scroll=5m&sort=_doc", body)
    } else {
      HttpUtils.post(httpClient, searchUrl, JsonUtils.serialize(Map("scroll" -> "5m", "scroll_id" -> body)))
    }
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    if(map.get("error").isDefined){
      throw new RuntimeException(map("error").toString)
    } else {
      val scrollId = map("_scroll_id").toString
      val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
      list match {
        case Nil if init == false => stream
        case Nil  => _scroll0(false, s"${url}/_search/scroll", scrollId, stream, invoker)
        case list => _scroll0(false, s"${url}/_search/scroll", scrollId, list.map { map => invoker(map("_id").toString, getDocumentMap(map)) }.toStream #::: stream, invoker)
      }
    }
  }

  def scrollChunk[T, R](config: ESConfig)(f: SearchDslBuilder => Unit)(p: (Seq[(String, T)]) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    logger.debug("******** ESConfig:" + config.toString)
    val builder = SearchDslBuilder.builder()
    f(builder)
    val json = builder.build()
    logger.debug(s"searchRequest:${json}")

    _scrollChunk0(true, config.url(url) + "/_search", json, Stream.empty,
      (maps: Seq[(String, Map[String, Any])]) => p(maps.map { case (id, map) =>
        (id, JsonUtils.deserialize[T](JsonUtils.serialize(map)))
      })
    )
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def scrollChunkByTemplate[T, R](config: ESConfig)(lang: String, template: String, params: AnyRef)(p: (Seq[(String, T)]) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    if(scriptTemplateIsAvailable) {
      logger.debug("******** ESConfig:" + config.toString)
      val json = JsonUtils.serialize(
        Map(
          "lang" -> lang,
          "file" -> template,
          "params" -> params
        )
      )
      logger.debug(s"searchRequest:${json}")

      _scrollChunk0(true, config.url(url) + "/_search/script_template", json, Stream.empty,
        (maps: Seq[(String, Map[String, Any])]) => p(maps.map { case (id, map) =>
          (id, JsonUtils.deserialize[T](JsonUtils.serialize(map)))
        })
      )

    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  @tailrec
  private def _scrollChunk0[R](init: Boolean, searchUrl: String, body: String, stream: Stream[R], invoker: (Seq[(String, Map[String, Any])]) => R): Stream[R] = {
    val resultJson = if(init){
      HttpUtils.post(httpClient, searchUrl + "?scroll=5m&sort=_doc", body)
    } else {
      HttpUtils.post(httpClient, searchUrl, JsonUtils.serialize(Map("scroll" -> "5m", "scroll_id" -> body)))
    }
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    if(map.get("error").isDefined){
      throw new RuntimeException(map("error").toString)
    } else {
      val scrollId = map("_scroll_id").toString
      val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
      list match {
        case Nil if init == false => stream
        case Nil  => _scrollChunk0(false, s"${url}/_search/scroll", scrollId, stream, invoker)
        case list => _scrollChunk0(false, s"${url}/_search/scroll", scrollId, Seq(invoker(list.map { map => (map("_id").toString, getDocumentMap(map)) })).toStream #::: stream, invoker)
      }
    }
  }

//  def scrollAsMap[R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: Map[String, Any] => R)(implicit c: ClassTag[R]): Stream[R] = {
//    logger.debug("******** ESConfig:" + config.toString)
//    val searcher = queryClient.prepareSearch(config.indexName)
//    config.typeName.foreach(x => searcher.setTypes(x))
//    f(searcher)
//    logger.debug(s"searchRequest:${searcher.toString}")
//
//    scroll0(config.url(url) + "/_search", searcher.toString, Stream.empty, (_id: String, map: Map[String, Any]) => p(map))
//  }

  def bulk[T](actions: Seq[BulkAction]): Either[Map[String, Any], Map[String, Any]] = {
    val resultJson = HttpUtils.post(
      httpClient  = httpClient,
      url         = s"${url}/_bulk", actions.map(_.jsonString).mkString("", "\n", "\n"),
      contentType = ContentType.XNDJSON
    )
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("errors").collect { case true => Left(map) }.getOrElse(Right(map))
  }

}
