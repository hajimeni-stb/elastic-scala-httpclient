package jp.co.bizreach.elasticsearch4s

case class ESConfig(indexName: String, typeName: Option[String] = None, preference: Option[String] = None, explain: Boolean = false, timeout: Option[Int] = None){

  /**
   * Returns the index URL.
   */
  def url(baseUrl: String) = typeName.map { typeName =>
    s"${baseUrl}/${indexName}/${typeName}"
  }.getOrElse {
    s"${baseUrl}/${indexName}"
  }

  /**
   * Returns the URL for document APIs (e.g. Index API, Delete API).
   */
  def documentUrl(baseUrl: String) = typeName.map { typeName =>
    s"${baseUrl}/${indexName}/${typeName}"
  }.getOrElse {
    s"${baseUrl}/${indexName}/_doc"
  }

  /**
   * Returns the URL with a preference parameter for search APIs.
   */
  def preferenceUrl(baseUrl: String, path: String) = {
    val u = url(baseUrl) + "/" + path
    u + preference.map { x =>
      (if(u.indexOf('?') >= 0) "&" else "?") + "preference=" + x
    }.getOrElse("")
  }

  /**
   * Returns the URL with request parameters for search APIs.
   */
  def urlWithParameters(baseUrl: String, path: String) = {
    val u = url(baseUrl) + "/" + path

    val u2 = u + preference.map { x =>
      (if(u.indexOf('?') >= 0) "&" else "?") + "preference=" + x
    }.getOrElse("")

    val u3 = if(explain) {
      u2 + (if(u2.indexOf('?') >= 0) "&" else "?") + "explain=true"
    } else {
      u2
    }

    u3 + timeout.map { x =>
      (if(u3.indexOf('?') >= 0) "&" else "?") + "timeout=" + x + "ms"
    }.getOrElse("")
  }
}

object ESConfig {

  /**
   * Creates ESConfig instance with index name and type name.
   */
  @deprecated("Types will be deprecated in APIs in Elasticsearch 7.0.0, and completely removed in 8.0.0.", "4.0.0")
  def apply(indexName: String, typeName: String): ESConfig = ESConfig(indexName, Some(typeName))

}