package jp.co.bizreach.elasticsearch4s

import java.io.File
import java.nio.file.Files

import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io._
import IntegrationTest._
import jp.co.bizreach.elasticsearch4s.retry.{FixedBackOff, RetryConfig, FutureRetryManager}
import org.apache.commons.io.IOUtils
import org.codelibs.elasticsearch.sstmpl.ScriptTemplatePlugin
import org.elasticsearch.common.settings.Settings.Builder
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner

import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  System.setSecurityManager(null) // to enable execution of script

  implicit val DefaultRetryConfig = RetryConfig(0, Duration.Zero, FixedBackOff)
  implicit val DefaultRetryManager = new FutureRetryManager()

  private var runner: ElasticsearchClusterRunner = null
  private var esHomeDir: File = null

  private def fromClasspath(path: String): String = {
    val in = Thread.currentThread.getContextClassLoader.getResourceAsStream(path)
    try {
      IOUtils.toString(in, "UTF-8")
    } finally {
      in.close()
    }
  }

  before {
    esHomeDir = File.createTempFile("eshome", "")
    esHomeDir.delete()

    val scriptDir = new File(esHomeDir, "config/node_1/scripts")
    scriptDir.mkdirs()

    val scriptFile = new File(scriptDir, "test_script.groovy")
    Files.write(scriptFile.toPath, fromClasspath("config/scripts/test_script.groovy").getBytes)

    runner = new ElasticsearchClusterRunner()
    runner.onBuild((number: Int, settingsBuilder: Builder) => {
        settingsBuilder.put("script.inline", "on")
        settingsBuilder.put("script.stored", "on")
        settingsBuilder.put("script.file", "on")
        settingsBuilder.put("script.search", "on")
        settingsBuilder.put("http.cors.enabled", true)
        settingsBuilder.put("http.cors.allow-origin", "*")
        settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9310")
      }
    ).build(ElasticsearchClusterRunner.newConfigs().baseHttpPort(9200).baseTransportPort(9300).numOfNode(1)
      .basePath(esHomeDir.getAbsolutePath())
      .pluginTypes(classOf[ScriptTemplatePlugin].getName))

    runner.ensureYellow()

    val client = HttpUtils.createHttpClient()
    HttpUtils.put(client, "http://localhost:9201/my_index", fromClasspath("schema.json"))
    client.close()

    ESClient.init()
    AsyncESClient.init()
  }

  after {
    runner.close()
    esHomeDir.delete()

    ESClient.shutdown()
    AsyncESClient.shutdown()
  }

  override def afterAll() = {
    DefaultRetryManager.shutdown()
  }

  test("Insert with id"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))

    client.refresh(config)

    val result = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("123"))
    }

    assert(result == Some("123", Blog("Hello World!", "This is a first registration test!", 20171013)))
  }

  test("Cluster health"){
    val client = ESClient("http://localhost:9201", true)

    assert(client.clusterHealth().get("cluster_name") == Some("elasticsearch-cluster-runner"))
  }

  test("Update partially"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data", 20171013))
    client.refresh(config)
    val registrationResult = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("1234"))
    }
    assert(registrationResult == Some("1234", Blog("Hello World!", "This is a registered data", 20171013)))

    client.updatePartially(config, "1234", BlogContent("This is a updated data"))
    client.refresh(config)
    val updateResult1 = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("1234"))
    }
    assert(updateResult1 == Some("1234", Blog("Hello World!", "This is a updated data", 20171013)))

    client.updatePartiallyJson(config, "1234", "{ \"subject\": \"Hello Scala!\" }")
    client.refresh(config)
    val updateResult2 = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("1234"))
    }
    assert(updateResult2 == Some("1234", Blog("Hello Scala!", "This is a updated data", 20171013)))
  }

  test("Error response"){
    val client = HttpUtils.createHttpClient()
    intercept[HttpResponseException] {
      // Create existing index to cause HttpResponseException
      HttpUtils.post(client, "http://localhost:9201/my_index",
        Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).mkString)
    }
    client.close()
  }

  test("Error response in async API"){
    val client = HttpUtils.createHttpClient()
    // Create existing index to cause HttpResponseException
    val f = HttpUtils.postAsync(client, "http://localhost:9201/my_index",
      Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).mkString)

    intercept[HttpResponseException] {
      Await.result(f, Duration.Inf)
    }
    client.close()
  }

  test("Sync client"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true)

    // Register 100 docs
    (1 to 100).foreach { num =>
      client.insert(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!",
        "date"    -> 20171013
      ))
    }
    client.refresh(config)

    // Check doc count
    val count1 = client.countAsInt(config){ builder =>
      builder.query(matchAllQuery)
    }
    assert(count1 == 100)

    // Check doc exists
    val result1 = client.find[Blog](config){ builder =>
      builder.query(matchPhraseQuery("subject", "10"))
    }
    assert(result1.get._2.subject == "[10]Hello World!")
    assert(result1.get._2.content == "This is a first registration test!")

    // Check doc exists (ESSearchResult / sorted)
    val result2 = client.list[Blog](config){ builder =>
      builder.query(matchPhraseQuery("subject", "10")).sort("date")
    }
    assert(result2.list.size == 1)
    assert(result2.list(0).doc == Blog("[10]Hello World!", "This is a first registration test!", 20171013))
    assert(result2.list(0).sort == Seq(20171013))

    // Check doc exists (ESSearchResult / not sorted)
    val result3 = client.list[Blog](config){ builder =>
      builder.query(matchPhraseQuery("subject", "10"))
    }
    assert(result3.list.size == 1)
    assert(result3.list(0).doc == Blog("[10]Hello World!", "This is a first registration test!", 20171013))
    assert(result3.list(0).sort == Nil)

    // Delete 1 doc
    client.deleteByQuery(config){ builder =>
      builder.query(matchPhraseQuery("subject", "10"))
    }
    client.refresh(config)

    // Check doc doesn't exist
    val result4 = client.find[Blog](config){ builder =>
      builder.query(matchPhraseQuery("subject", "10"))
    }
    assert(result4.isEmpty)

    // Check doc count
    val count2 = client.countAsInt(config){ builder =>
      builder.query(matchAllQuery)
    }
    assert(count2 == 99)

    // Scroll search
    val sum = client.scroll[Blog, Int](config){ builder =>
      builder.query(matchPhraseQuery("subject", "Hello"))
    }{ case (id, blog) =>
      assert(blog.content == "This is a first registration test!")
      1
    }.sum
    assert(sum == 99)

    // Scroll search (chunk)
    val chunkSum = client.scrollChunk[Blog, Int](config){ builder =>
      builder.query(matchPhraseQuery("subject", "Hello"))
    }{ chunk =>
      chunk.map { case (id, blog) =>
        assert(blog.content == "This is a first registration test!")
        1
      }.sum
    }.sum
    assert(chunkSum == 99)

    // Count by template
    val count3 = client.countByTemplateAsInt(config)(
      lang = "groovy",
      template = "test_script",
      params = Map("subjectValue" -> "Hello")
    )
    assert(count3 === 99)

    // Scroll by template
    var count4 = 0
    client.scrollByTemplate[Map[String, Any], Unit](config)(
      lang = "groovy",
      template = "test_script",
      params = Map("subjectValue" -> "Hello")
    ){ case (id, doc) =>
      assert(doc("content") == "This is a first registration test!")
      count4 = count4 + 1
    }
    assert(count4 == 99)

    // Scroll chunk by template
    var count5 = 0
    client.scrollChunkByTemplate[Map[String, Any], Unit](config)(
      lang = "groovy",
      template = "test_script",
      params = Map("subjectValue" -> "Hello")
    ){ docs =>
      docs.foreach { case (id, doc) =>
        assert(doc("content") == "This is a first registration test!")
        count5 = count5 + 1
      }
    }
    assert(count5 == 99)

    // Scroll by Json search
    val jsonRequest = """{"query": {"match_all": {}}, "size": 1}"""
    val sum2 = client.scrollJson[Blog, Int](config, jsonRequest){ case (id, blog) =>
      assert(blog.content == "This is a first registration test!")
      1
    }.sum
    assert(sum2 == 99)
  }

  test("noFields"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true)

    // Register 100 docs
    (1 to 100).foreach { num =>
      client.insert(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
      ))
    }
    client.refresh(config)


    val result = client.scroll[Unit, Unit](config){ builder =>
      builder.query(matchAllQuery) // no fields??
    }{ case (id, x) => x }

    assert(result.size == 100)
    assert(result.forall { _ == ((): Unit) })
  }

  test("Async client"){
    val config = ESConfig("my_index", "my_type")

    val httpClient = HttpUtils.createHttpClient()
    val client = new AsyncESClient(httpClient, "http://localhost:9201", true)

    val seqf = (1 to 100).map { num =>
      client.insertAsync(config, num.toString, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
      ))
    }

    val f1 = for {
      _ <- Future.sequence(seqf)
      _ <- client.refreshAsync(config)
      count <- client.countAsIntAsync(config) { builder =>
        builder.query(matchAllQuery)
      }
    } yield count

    val count1 = Await.result(f1, Duration.Inf)
    assert(count1 == 100)

    val f2 = for {
      list <- client.findAllAsListAsync[Unit](config) { builder =>
        builder.query(matchAllQuery)
      }
    } yield list

    val list1 = Await.result(f2, Duration.Inf)
    assert(list1.size == 100)

    val f3 = for {
      result <- client.listAllAsync[Unit](config) { builder =>
        builder.query(matchAllQuery)
      }
    } yield result

    val result1 = Await.result(f3, Duration.Inf)
    assert(result1.totalHits == 100)
    assert(result1.list.size == 100)

    var count2 = 0
    val f4 = for {
      _ <- client.deleteAsync(config, "1")
      _ <- client.refreshAsync(config)
      _ <- client.scrollByTemplateAsync[Map[String, Any], Unit](config)(
        lang = "groovy",
        template = "test_script",
        params = Map("subjectValue" -> "Hello")
      ){ case (id, doc) => count2 = count2 + 1 }
    } yield ()

    Await.result(f4, Duration.Inf)
    assert(count2 == 99)

    var count3 = 0
    val f5 = client.scrollChunkByTemplateAsync[Map[String, Any], Unit](config)(
      lang = "groovy",
      template = "test_script",
      params = Map("subjectValue" -> "Hello")
    ){ docs => docs.foreach {
      case (id, doc) => count3 = count3 + 1
    }}

    Await.result(f5, Duration.Inf)
    assert(count3 == 99)

    httpClient.close()
  }

  test("Async cluster health"){
    val config = ESConfig("my_index", "my_type")
    val client = AsyncESClient("http://localhost:9201")

    val result = client.clusterHealthAsync(config)

    val clusterHealth = Await.result(result, Duration.Inf)
    assert(clusterHealth.get("cluster_name") == Some("elasticsearch-cluster-runner"))
  }

}

object IntegrationTest {
  case class Blog(subject: String, content: String, date: Int)
  case class BlogContent(content: String)
}