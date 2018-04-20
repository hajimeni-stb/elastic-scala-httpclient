package jp.co.bizreach.elasticsearch4s

import ESUtils._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import jp.co.bizreach.elasticsearch4s.retry.{FixedBackOff, RetryConfig, RetryManager}
import org.codelibs.elasticsearch.querybuilders.SearchDslBuilder

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object AsyncESClient {
  private var httpClient: AsyncHttpClient = null

  private val retryManager: RetryManager = new RetryManager()
  sys.ShutdownHookThread {
    retryManager.shutdown()
  }

  def using[T](
    url: String,
    scriptTemplateIsAvailable: Boolean = false,
    retryConfig: RetryConfig = RetryConfig(0, Duration.Zero, FixedBackOff)
  )(f: AsyncESClient => Future[T]): Future[T] = {
    val httpClient = HttpUtils.createHttpClient()
    val client = new AsyncESClient(httpClient, url, scriptTemplateIsAvailable)(retryConfig, retryManager)
    val future = f(client)
    future.onComplete { case t =>
      httpClient.close()
    }
    future
  }

  def apply(
    url: String,
    scriptTemplateIsAvailable: Boolean = false,
    retryConfig: RetryConfig = RetryConfig(0, Duration.Zero, FixedBackOff)
  ): AsyncESClient = {
    if(httpClient == null){
      throw new IllegalStateException("AsyncHttpClient has not been initialized. Call AsyncESClient.init() at first.")
    }
    new AsyncESClient(httpClient, url, scriptTemplateIsAvailable)(retryConfig, retryManager)
  }

  def init() = {
    httpClient = HttpUtils.createHttpClient()
  }

  def init(config: AsyncHttpClientConfig) = {
    httpClient = HttpUtils.createHttpClient(config)
  }

  def shutdown() = {
    httpClient.close()
  }
}

class AsyncESClient(httpClient: AsyncHttpClient, url: String, scriptTemplateIsAvailable: Boolean = false)
                   (implicit retryConfig: RetryConfig, retryManager: RetryManager) {

  val logger = LoggerFactory.getLogger(classOf[AsyncESClient])

  def searchAsync(config: ESConfig)(f: SearchDslBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val builder = SearchDslBuilder.builder()
    f(builder)
    val json = builder.build()
    logger.debug(s"searchRequest:${json}")

    val future = HttpUtils.postAsync(httpClient, config.preferenceUrl(url, "_search"), json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  def searchAllAsync(config: ESConfig)(f: SearchDslBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = {
    countAsync(config)(f).flatMap { result =>
      result match {
        case Left(x)  => Future(Left(x))
        case Right(x) => {
          val total = x("count").asInstanceOf[Int]
          searchAsync(config) { builder =>
            f(builder)
            builder.from(0).size(total)
          }
        }
      }
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def searchByTemplateAsync(config: ESConfig)(lang: String, template: String, params: AnyRef, options: Option[String] = None): Future[Either[Map[String, Any], Map[String, Any]]] = {
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

      val future = HttpUtils.postAsync(httpClient, config.urlWithParameters(url, "_search/script_template" + options.getOrElse("")), json)
      future.map { resultJson =>
        val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
        map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def findAsync[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): Future[Option[(String, T)]] = {
    searchAsync(config)(f).map { result =>
      result match {
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
  }

  def findAsListAsync[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): Future[List[(String, T)]] = {
    searchAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => createESSearchResult(x).list.map { x => (x.id, x.doc) }
      }
    }
  }

  def findAllAsListAsync[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): Future[List[(String, T)]] = {
    findAsListAsync(config){ builder =>
      countAsIntAsync(config)(f).map { count =>
        f(builder)
        builder.from(0).size(count)
      }
    }
  }

  def listAsync[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): Future[ESSearchResult[T]] = {
    searchAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => createESSearchResult(x)
      }
    }
  }

  def listAllAsync[T](config: ESConfig)(f: SearchDslBuilder => Unit)(implicit c: ClassTag[T]): Future[ESSearchResult[T]] = {
    listAsync(config){ builder =>
      countAsIntAsync(config)(f).map { count =>
        f(builder)
        builder.from(0).size(count)
      }
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def listByTemplateAsync[T](config: ESConfig)(lang: String, template: String, params: AnyRef)(implicit c: ClassTag[T]): Future[ESSearchResult[T]] = {
    if(scriptTemplateIsAvailable){
      searchByTemplateAsync(config)(lang, template, params).map { result =>
        result match {
          case Left(x)  => throw new RuntimeException(x("error").toString)
          case Right(x) => createESSearchResult(x)
        }
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def insertJsonAsync(config: ESConfig, json: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"insertJson:\n${json}")

    val future = HttpUtils.postAsync(httpClient, config.url(url), json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  def insertJsonAsync(config: ESConfig, id: String, json: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"insertJson:\n${json}")

    val future = HttpUtils.postAsync(httpClient, config.url(url) + "/" + id, json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  def insertAsync(config: ESConfig, entity: AnyRef):  Future[Either[Map[String, Any], Map[String, Any]]] = {
    insertJsonAsync(config, JsonUtils.serialize(entity))
  }

  def insertAsync(config: ESConfig, id: String, entity: AnyRef):  Future[Either[Map[String, Any], Map[String, Any]]] = {
    insertJsonAsync(config, id, JsonUtils.serialize(entity))
  }

  def updateJsonAsync(config: ESConfig, id: String, json: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"updateJson:\n${json}")

    val future = HttpUtils.putAsync(httpClient, config.url(url) + "/" + id, json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  def updateAsync(config: ESConfig, id: String, entity: AnyRef): Future[Either[Map[String, Any], Map[String, Any]]] = {
    updateJsonAsync(config, id, JsonUtils.serialize(entity))
  }

  def deleteAsync(config: ESConfig, id: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"delete id:\n${id}")

    val future = HttpUtils.deleteAsync(httpClient, config.url(url) + "/" + id)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  //def deleteByQueryAsync(config: ESConfig)(f: SearchRequestBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = ???

  def countAsync(config: ESConfig)(f: SearchDslBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val builder = SearchDslBuilder.builder()
    f(builder)
    val json = builder.build()
    logger.debug(s"countRequest:${json}")

    val future = HttpUtils.postAsync(httpClient, config.preferenceUrl(url, "_count"), json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  def countAsIntAsync(config: ESConfig)(f: SearchDslBuilder => Unit): Future[Int] = {
    countAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => x("count").asInstanceOf[Int]
      }
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def countByTemplateAsync(config: ESConfig)(lang: String, template: String, params: AnyRef): Future[Either[Map[String, Any], Map[String, Any]]] = {
    if(scriptTemplateIsAvailable){
      searchByTemplateAsync(config)(lang, template, params, Some("?search_type=query_then_fetch&size=0"))
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def countByTemplateAsIntAsync(config: ESConfig)(lang: String, template: String, params: AnyRef): Future[Int] = {
    if(scriptTemplateIsAvailable){
      countByTemplateAsync(config)(lang: String, template: String, params: AnyRef).map { result =>
        result match {
          case Left(x)  => throw new RuntimeException(x("error").toString)
          case Right(x) => x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
        }
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def refreshAsync(config: ESConfig)(): Future[Either[Map[String, Any], Map[String, Any]]] = {
    val future = HttpUtils.postAsync(httpClient, s"${url}/${config.indexName}/_refresh", "")
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { _ => Left(map) }.getOrElse(Right(map))
    }
  }

  def clusterHealthAsync(config: ESConfig): Future[Map[String, Any]] = {
    val future = HttpUtils.getAsync(httpClient, s"${url}/_cluster/health")
    future.map { resultJson =>
      JsonUtils.deserialize[Map[String, Any]](resultJson)
    }
  }

  def scrollAsync[T, R](config: ESConfig)(f: SearchDslBuilder => Unit)(p: (String, T) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Future[Stream[R]] = {
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
  def scrollByTemplateAsync[T, R](config: ESConfig)(lang: String, template: String, params: AnyRef)(p: (String, T) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Future[Stream[R]] = {
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

  private def _scroll0[R](init: Boolean, searchUrl: String, body: String, stream: Stream[R], invoker: (String, Map[String, Any]) => R): Future[Stream[R]] = {
    val future = if(init){
      HttpUtils.postAsync(httpClient, searchUrl + "?scroll=5m&sort=_doc", body)
    } else {
      HttpUtils.postAsync(httpClient, searchUrl, JsonUtils.serialize(Map("scroll" -> "5m", "scroll_id" -> body)))
    }
    future.flatMap { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      if(map.get("error").isDefined){
        throw new RuntimeException(map("error").toString)
      } else {
        val scrollId = map("_scroll_id").toString
        val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
        list match {
          case Nil if init == false => Future(stream)
          case Nil  => _scroll0(false, s"${url}/_search/scroll", scrollId, stream, invoker)
          case list => _scroll0(false, s"${url}/_search/scroll", scrollId, list.map { map => invoker(map("_id").toString, getDocumentMap(map)) }.toStream #::: stream, invoker)
        }
      }
    }
  }

  def scrollChunkAsync[T, R](config: ESConfig)(f: SearchDslBuilder => Unit)(p: (Seq[(String, T)]) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Future[Stream[R]] = {
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
  def scrollChunkByTemplateAsync[T, R](config: ESConfig)(lang: String, template: String, params: AnyRef)(p: (Seq[(String, T)]) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Future[Stream[R]] = {
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

  private def _scrollChunk0[R](init: Boolean, searchUrl: String, body: String, stream: Stream[R], invoker: (Seq[(String, Map[String, Any])]) => R): Future[Stream[R]] = {
    val future = if(init){
      HttpUtils.postAsync(httpClient, searchUrl + "?scroll=5m&sort=_doc", body)
    } else {
      HttpUtils.postAsync(httpClient, searchUrl, JsonUtils.serialize(Map("scroll" -> "5m", "scroll_id" -> body)))
    }
    future.flatMap { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      if(map.get("error").isDefined){
        throw new RuntimeException(map("error").toString)
      } else {
        val scrollId = map("_scroll_id").toString
        val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
        list match {
          case Nil if init == false => Future(stream)
          case Nil  => _scrollChunk0(false, s"${url}/_search/scroll", scrollId, stream, invoker)
          case list => _scrollChunk0(false, s"${url}/_search/scroll", scrollId, Seq(invoker(list.map { map => (map("_id").toString, getDocumentMap(map)) })).toStream #::: stream, invoker)
        }
      }
    }
  }

  def bulkAsync[T](actions: Seq[BulkAction]): Future[Either[Map[String, Any], Map[String, Any]]] = {
    val future = HttpUtils.postAsync(
      httpClient  = httpClient,
      url         = s"${url}/_bulk", actions.map(_.jsonString).mkString("", "\n", "\n"),
      contentType = ContentType.XNDJSON
    )
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("errors").collect { case true => Left(map) }.getOrElse(Right(map))
    }
  }

}
