package jp.co.bizreach.elasticsearch4s

import java.io.File

import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io._
import IntegrationTest._
import jp.co.bizreach.elasticsearch4s.retry.{FixedBackOff, RetryConfig, FutureRetryManager}
import org.codelibs.elasticsearch.sstmpl.ScriptTemplatePlugin
import org.elasticsearch.common.settings.Settings.Builder
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner

import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  implicit val DefaultRetryConfig = RetryConfig(0, Duration.Zero, FixedBackOff)
  implicit val DefaultRetryManager = new FutureRetryManager()

  private var runner: ElasticsearchClusterRunner = null
  private var esHomeDir: File = null

  private def fromClasspath(path: String): String = {
    val in = Thread.currentThread.getContextClassLoader.getResourceAsStream(path)
    try {
      Source.fromInputStream(in, "UTF-8").mkString
    } finally {
      in.close()
    }
  }

  before {
    esHomeDir = File.createTempFile("eshome", "")
    esHomeDir.delete()

    runner = new ElasticsearchClusterRunner()
    runner.onBuild((number: Int, settingsBuilder: Builder) => {
        settingsBuilder.put("http.cors.enabled", true)
        settingsBuilder.put("http.cors.allow-origin", "*")
      }
    ).build(ElasticsearchClusterRunner.newConfigs().baseHttpPort(9200).numOfNode(1)
      .basePath(esHomeDir.getAbsolutePath())
      .pluginTypes(classOf[ScriptTemplatePlugin].getName))

    runner.ensureYellow()

    val client = HttpUtils.createHttpClient()
    HttpUtils.put(client, "http://localhost:9201/my_index", fromClasspath("schema.json"))
    HttpUtils.post(client, "http://localhost:9201/_scripts/test_script",
      s"""{ "script" : { "lang" : "mustache", "source" : ${fromClasspath("config/scripts/test_script.mustache")} }}""")
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

  test("Insert doc for explicit ids"){
    val config = ESConfig("my_index")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))

    client.refresh(config)

    val result = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("123"))
    }

    assert(result == Some("123", Blog("Hello World!", "This is a first registration test!", 20171013)))
  }

  test("Cluster health"){
    val client = ESClient("http://localhost:9201", true)

    assert(client.clusterHealth().get("cluster_name") == Some("elasticsearch-cluster-runner"))
  }

  test("Update doc for explicit ids"){
    val config = ESConfig("my_index")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data", 20171013))
    client.refresh(config)
    val data = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(data == Some("1234", Blog("Hello World!", "This is a registered data", 20171013)))

    client.update(config, "1234", BlogContent("This is an updated data"))
    client.refresh(config)
    val result = client.find[BlogContent](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(result == Some("1234", BlogContent("This is an updated data")))
  }

  test("Update partially"){
    val config = ESConfig("my_index")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data", 20171013))
    client.refresh(config)
    val data = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(data == Some("1234", Blog("Hello World!", "This is a registered data", 20171013)))

    client.updatePartially(config, "1234", BlogContent("This is an updated data"))
    client.refresh(config)
    val result = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(result == Some("1234", Blog("Hello World!", "This is an updated data", 20171013)))

    client.updatePartiallyJson(config, "1234", "{ \"subject\": \"Hello Scala!\" }")
    client.refresh(config)
    val result2 = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(result2 == Some("1234", Blog("Hello Scala!", "This is an updated data", 20171013)))
  }

  test("Delete doc for explicit ids"){
    val config = ESConfig("my_index")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data", 20171013))
    client.refresh(config)
    val data = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(data == Some("1234", Blog("Hello World!", "This is a registered data", 20171013)))

    client.delete(config, "1234")
    client.refresh(config)
    // Check doc doesn't exist
    val result = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(result.isEmpty)
  }

  test("Delete by query"){
    val config = ESConfig("my_index")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data", 20171013))
    client.refresh(config)
    val data = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(data == Some("1234", Blog("Hello World!", "This is a registered data", 20171013)))

    client.deleteByQuery(config){ builder =>
      builder.query(matchPhraseQuery("subject", "Hello"))
    }
    client.refresh(config)
    // Check doc doesn't exist
    val result = client.find[Blog](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }
    assert(result.isEmpty)
  }

  test("Error response"){
    val client = HttpUtils.createHttpClient()
    intercept[HttpResponseException] {
      // Create existing index to cause HttpResponseException
      HttpUtils.post(client, "http://localhost:9201/my_index", fromClasspath("schema.json"))
    }
    client.close()
  }

  test("Error response in async API"){
    val client = HttpUtils.createHttpClient()
    // Create existing index to cause HttpResponseException
    val f = HttpUtils.postAsync(client, "http://localhost:9201/my_index", fromClasspath("schema.json"))

    intercept[HttpResponseException] {
      Await.result(f, Duration.Inf)
    }
    client.close()
  }

  test("Sync client"){
    val config = ESConfig("my_index")
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
    {
      val count = client.countAsInt(config){ builder =>
        builder.query(matchAllQuery)
      }
      assert(count == 100)
    }

    // Check doc exists
    {
      val result = client.find[Blog](config){ builder =>
        builder.query(matchPhraseQuery("subject", "10"))
      }
      assert(result.get._2.subject == "[10]Hello World!")
      assert(result.get._2.content == "This is a first registration test!")
    }

    // Check doc exists (ESSearchResult / sorted)
    {
      val result = client.list[Blog](config){ builder =>
        builder.query(matchPhraseQuery("subject", "10")).sort("date")
      }
      assert(result.list.size == 1)
      assert(result.list(0).doc == Blog("[10]Hello World!", "This is a first registration test!", 20171013))
      assert(result.list(0).sort == Seq(20171013))
    }

    // Check doc exists (ESSearchResult / not sorted)
    {
      val result = client.list[Blog](config){ builder =>
        builder.query(matchPhraseQuery("subject", "10"))
      }
      assert(result.list.size == 1)
      assert(result.list(0).doc == Blog("[10]Hello World!", "This is a first registration test!", 20171013))
      assert(result.list(0).sort == Nil)
      assert(result.total.value == 1)
      assert(result.total.relation == "eq")
    }

    // Scroll search
    {
      val sum = client.scroll[Blog, Int](config){ builder =>
        builder.query(matchPhraseQuery("subject", "Hello"))
      }{ case (id, blog) =>
        assert(blog.content == "This is a first registration test!")
        1
      }.sum
      assert(sum == 100)
    }

    // Scroll search (chunk)
    {
      val chunkSum = client.scrollChunk[Blog, Int](config){ builder =>
        builder.query(matchPhraseQuery("subject", "Hello"))
      }{ chunk =>
        chunk.map { case (id, blog) =>
          assert(blog.content == "This is a first registration test!")
          1
        }.sum
      }.sum
      assert(chunkSum == 100)
    }

    // Count by template
    {
      val count = client.countByTemplateAsInt(config)(
        template = "test_script",
        params = Map("subjectValue" -> "Hello")
      )
      assert(count === 100)
    }

    // Scroll by template
    {
      var count = 0
      client.scrollByTemplate[Map[String, Any], Unit](config)(
        template = "test_script",
        params = Map("subjectValue" -> "Hello")
      ){ case (id, doc) =>
        assert(doc("content") == "This is a first registration test!")
        count = count + 1
      }
      assert(count == 100)
    }

    // Scroll chunk by template
    {
      var count = 0
      client.scrollChunkByTemplate[Map[String, Any], Unit](config)(
        template = "test_script",
        params = Map("subjectValue" -> "Hello")
      ){ docs =>
        docs.foreach { case (id, doc) =>
          assert(doc("content") == "This is a first registration test!")
          count = count + 1
        }
      }
      assert(count == 100)
    }

    // Scroll by Json search
    {
      val jsonRequest = """{"query": {"match_all": {}}, "size": 1}"""
      val sum = client.scrollJson[Blog, Int](config, jsonRequest){ case (id, blog) =>
        assert(blog.content == "This is a first registration test!")
        1
      }.sum
      assert(sum == 100)
    }
  }

  test("noFields"){
    val config = ESConfig("my_index")
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
  }

  test("Bulk API"){
    val config = ESConfig("my_index")
    val client = ESClient("http://localhost:9201", true)

    client.insert(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))
    client.insert(config, "456", Blog("Hello World!", "This is a first registration test!", 20171013))
    client.refresh(config)

    val result = client.bulk(Seq(
      BulkAction.Index(config, Blog("Hello Java", "created", 20190401)),
      BulkAction.Create(config, Blog("Hello Python", "created", 20190401), "789"),
      BulkAction.Update(config, Blog("Hello Scala", "updated", 20190401), "123"),
      BulkAction.Delete(config, "456")
    ))
    assert(result.isRight)
    assert(result.exists(_("items").asInstanceOf[List[_]].size == 4))

    client.refresh(config)
    val all = client.findAllAsList[Blog](config){ builder =>
      builder.query(matchAllQuery)
    }.toMap

    assert(all.size == 3)
    assert(all("123") == Blog("Hello Scala", "updated", 20190401))
    assert(all("789") == Blog("Hello Python", "created", 20190401))
    assert(all.exists(_._2 == Blog("Hello Java", "created", 20190401)))
  }

  test("Async insert doc for explicit ids"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9201", true)

    val f = for {
      _ <- client.insertAsync(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))
      _ <- client.refreshAsync(config)
      result <- client.findAsync[Blog](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val result = Await.result(f, Duration.Inf)
    assert(result == Some("123", Blog("Hello World!", "This is a first registration test!", 20171013)))
  }

  test("Async cluster health"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9201")

    val result = client.clusterHealthAsync(config)

    val clusterHealth = Await.result(result, Duration.Inf)
    assert(clusterHealth.get("cluster_name") == Some("elasticsearch-cluster-runner"))
  }

  test("Async update doc for explicit ids"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9201", true)

    val f = for {
      _ <- client.insertAsync(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))
      _ <- client.refreshAsync(config)
      result <- client.findAsync[Blog](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val data = Await.result(f, Duration.Inf)
    assert(data == Some("123", Blog("Hello World!", "This is a first registration test!", 20171013)))

    val f2 = for {
      _ <- client.updateAsync(config, "123", BlogContent("This is an updated data"))
      _ <- client.refreshAsync(config)
      result <- client.findAsync[BlogContent](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val result = Await.result(f2, Duration.Inf)
    assert(result == Some("123", BlogContent("This is an updated data")))
  }

  test("Async delete doc for explicit ids"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9201", true)

    val f = for {
      _ <- client.insertAsync(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))
      _ <- client.refreshAsync(config)
      result <- client.findAsync[Blog](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val data = Await.result(f, Duration.Inf)
    assert(data == Some("123", Blog("Hello World!", "This is a first registration test!", 20171013)))

    val f2 = for {
      _ <- client.deleteAsync(config, "123")
      _ <- client.refreshAsync(config)
      result <- client.findAsync[Blog](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val result = Await.result(f2, Duration.Inf)
    assert(result.isEmpty)
  }

  test("Async delete by query"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9201", true)

    val f = for {
      _ <- client.insertAsync(config, "123", Blog("Hello World!", "This is a first registration test!", 20171013))
      _ <- client.refreshAsync(config)
      result <- client.findAsync[Blog](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val data = Await.result(f, Duration.Inf)
    assert(data == Some("123", Blog("Hello World!", "This is a first registration test!", 20171013)))

    val f2 = for {
      _ <- client.deleteByQueryAsync(config){ builder =>
        builder.query(idsQuery().addIds("123"))
      }
      _ <- client.refreshAsync(config)
      result <- client.findAsync[Blog](config) { builder =>
        builder.query(idsQuery().addIds("123"))
      }
    } yield result

    val result = Await.result(f2, Duration.Inf)
    assert(result.isEmpty)
  }

  test("Async client"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9201", true)

    // Register 100 docs
    val seqf = (1 to 100).map { num =>
      client.insertAsync(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
      ))
    }

    // Check doc count
    {
      val f = for {
        _ <- Future.sequence(seqf)
        _ <- client.refreshAsync(config)
        count <- client.countAsIntAsync(config) { builder =>
          builder.query(matchAllQuery)
        }
      } yield count

      val count = Await.result(f, Duration.Inf)
      assert(count == 100)
    }

    // Check doc exists
    {
      val f = for {
        list <- client.findAllAsListAsync[Unit](config) { builder =>
          builder.query(matchAllQuery)
        }
      } yield list

      val list = Await.result(f, Duration.Inf)
      assert(list.size == 100)
    }

    // Check doc exists
    {
      val f = for {
        result <- client.listAllAsync[Unit](config) { builder =>
          builder.query(matchAllQuery)
        }
      } yield result

      val result = Await.result(f, Duration.Inf)
      assert(result.totalHits == 100)
      assert(result.list.size == 100)
    }

    // Scroll search
    {
      val f = client.scrollAsync[Unit, Int](config){ builder =>
        builder.query(matchAllQuery)
      }{ case (id, _) =>
        1
      }.map(_.sum)

      val sum = Await.result(f, Duration.Inf)
      assert(sum == 100)
    }

    // Scroll search (chunk)
    {
      val f = client.scrollChunkAsync[Unit, Int](config){ builder =>
        builder.query(matchAllQuery)
      }{ chunk =>
        chunk.size
      }.map(_.sum)

      val chunkSum = Await.result(f, Duration.Inf)
      assert(chunkSum == 100)
    }

    // Count by template
    {
      val f = client.countByTemplateAsIntAsync(config)(
        template = "test_script",
        params = Map("subjectValue" -> "Hello")
      )
      val count = Await.result(f, Duration.Inf)
      assert(count === 100)
    }

    // Scroll by template
    {
      var count = 0
      val f = for {
        _ <- client.scrollByTemplateAsync[Map[String, Any], Unit](config)(
          template = "test_script",
          params = Map("subjectValue" -> "Hello")
        ){ case (id, doc) => count = count + 1 }
      } yield ()

      Await.result(f, Duration.Inf)
      assert(count == 100)
    }

    // Scroll chunk by template
    {
      var count = 0
      val f = client.scrollChunkByTemplateAsync[Map[String, Any], Unit](config)(
        template = "test_script",
        params = Map("subjectValue" -> "Hello")
      ){ docs => docs.foreach {
        case (id, doc) => count = count + 1
      }}

      Await.result(f, Duration.Inf)
      assert(count == 100)
    }
  }

}

object IntegrationTest {
  case class Blog(subject: String, content: String, date: Int)
  case class BlogContent(content: String)
}