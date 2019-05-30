package jp.co.bizreach.elasticsearch4s

import java.nio.charset.StandardCharsets

import org.asynchttpclient.{AsyncCompletionHandler, AsyncHttpClient, AsyncHttpClientConfig, Response}
import org.asynchttpclient.Dsl._

import scala.concurrent._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import retry._

class HttpResponseException(status: Int, headers: Seq[(String, String)], body: String)
  extends RuntimeException(
    s"HTTP response is bad. Response status: ${status}\n" +
    "---- headers ----\n" +
    headers.map { case (key, value) => s"${key}: ${value}" }.mkString("\n") + "\n" +
    "---- body ----\n" +
    body
  ){

  def this(response: Response) = {
    this(
      status  = response.getStatusCode,
      headers = response.getHeaders.entries().asScala.map { entry => (entry.getKey, entry.getValue) },
      body    = response.getResponseBody
    )
  }
}

object ContentType {
  val JSON = "application/json"
  val XNDJSON = "application/x-ndjson"
}

object HttpUtils {



  def createHttpClient(): AsyncHttpClient = {
    asyncHttpClient()
  }

  def createHttpClient(config: AsyncHttpClientConfig): AsyncHttpClient = {
    asyncHttpClient(config)
  }

  def closeHttpClient(httpClient: AsyncHttpClient): Unit = {
    httpClient.close()
  }

  def put(httpClient: AsyncHttpClient, url: String, json: String, contentType: String = ContentType.JSON)
         (implicit retryConfig: RetryConfig): String = {
    retryBlocking {
      val f = httpClient.preparePut(url).setHeader("Content-Type", contentType)
        .setBody(json.getBytes("UTF-8")).execute()
      val response = f.get()
      if (response.getStatusCode >= 200 && response.getStatusCode < 300){
        response.getResponseBody(StandardCharsets.UTF_8)
      } else {
        throw new HttpResponseException(response)
      }
    }
  }

  def putAsync(httpClient: AsyncHttpClient, url: String, json: String, contentType: String = ContentType.JSON)
              (implicit retryConfig: RetryConfig, retryManager: FutureRetryManager, ec: ExecutionContext): Future[String] = {
    retryFuture {
      withAsyncResultHandler { handler =>
        httpClient.preparePut(url).setHeader("Content-Type", contentType)
          .setBody(json.getBytes("UTF-8")).execute(handler)
      }
    }
  }

  def post(httpClient: AsyncHttpClient, url: String, json: String, contentType: String = ContentType.JSON)
          (implicit retryConfig: RetryConfig): String = {
    retryBlocking {
      val f = httpClient.preparePost(url).setHeader("Content-Type", contentType)
        .setBody(json.getBytes("UTF-8")).execute()
      val response = f.get()
      if (response.getStatusCode >= 200 && response.getStatusCode < 300) {
        response.getResponseBody(StandardCharsets.UTF_8)
      } else {
        throw new HttpResponseException(response)
      }
    }
  }

  def postAsync(httpClient: AsyncHttpClient, url: String, json: String, contentType: String = ContentType.JSON)
               (implicit retryConfig: RetryConfig, retryManager: FutureRetryManager, ec: ExecutionContext): Future[String] = {
    retryFuture {
      withAsyncResultHandler { handler =>
        httpClient.preparePost(url).setHeader("Content-Type", contentType)
          .setBody(json.getBytes("UTF-8")).execute(handler)
      }
    }
  }

  def get(httpClient: AsyncHttpClient, url: String)(implicit retryConfig: RetryConfig): String = {
    retryBlocking {
      val f = httpClient.prepareGet(url).execute()
      val response = f.get()
      if (response.getStatusCode >= 200 && response.getStatusCode < 300) {
        response.getResponseBody(StandardCharsets.UTF_8)
      } else {
        throw new HttpResponseException(response)
      }
    }
  }

  def getAsync(httpClient: AsyncHttpClient, url: String)
              (implicit retryConfig: RetryConfig, retryManager: FutureRetryManager, ec: ExecutionContext): Future[String] = {
    retryFuture {
      withAsyncResultHandler { handler =>
        httpClient.prepareGet(url).execute(handler)
      }
    }
  }

  def delete(httpClient: AsyncHttpClient, url: String, json: String = "", contentType: String = ContentType.JSON)
            (implicit retryConfig: RetryConfig): String = {
    retryBlocking {
      val builder = httpClient.prepareDelete(url)
      if (json.nonEmpty) {
        builder.setHeader("Content-Type", contentType).setBody(json.getBytes("UTF-8"))
      }
      val f = builder.execute()
      f.get().getResponseBody(StandardCharsets.UTF_8)
    }
  }

  def deleteAsync(httpClient: AsyncHttpClient, url: String, json: String = "", contentType: String = ContentType.JSON)
                 (implicit retryConfig: RetryConfig, retryManager: FutureRetryManager, ec: ExecutionContext): Future[String] = {
    retryFuture {
      withAsyncResultHandler { handler =>
        val builder = httpClient.prepareDelete(url)
        if (json.nonEmpty) {
          builder.setHeader("Content-Type", contentType).setBody(json.getBytes("UTF-8"))
        }
        builder.execute(handler)
      }
    }
  }

  private def withAsyncResultHandler(requestAsync: AsyncResultHandler => Unit): Future[String] = {
    try {
      val promise = Promise[String]()
      requestAsync(new AsyncResultHandler(promise))
      promise.future
    } catch {
      case NonFatal(th) => Future.failed(th)
    }
  }

  private class AsyncResultHandler(promise: Promise[String]) extends AsyncCompletionHandler[Unit] {
    override def onCompleted(response: Response): Unit = {
      try {
        if (response.getStatusCode >= 200 && response.getStatusCode < 300) {
          promise.success(response.getResponseBody(StandardCharsets.UTF_8))
        } else {
          promise.failure(new HttpResponseException(response))
        }
      } catch {
        case NonFatal(t) => promise.tryFailure(t)
      }
    }
    override def onThrowable(t: Throwable): Unit = {
      promise.failure(t)
    }
  }
}
