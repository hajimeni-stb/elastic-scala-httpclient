package jp.co.bizreach.elasticsearch4s

//>>>>>>> master
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io._
import IntegrationTest._
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.elasticsearch.script.groovy.GroovyPlugin
import org.codelibs.elasticsearch.sstmpl.ScriptTemplatePlugin

import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends FunSuite with BeforeAndAfter {

  System.setSecurityManager(null) // to enable execution of script
  private var runner: ElasticsearchClusterRunner = null

  before {
    runner = new ElasticsearchClusterRunner()
    runner.build(ElasticsearchClusterRunner.newConfigs().baseHttpPort(9200).baseTransportPort(9300).numOfNode(1))
//      .pluginTypes(Seq(
//        classOf[GroovyPlugin].getName,
//        classOf[ScriptTemplatePlugin].getName
//      ).mkString(",")))
    runner.ensureYellow()



    val client = HttpUtils.createHttpClient()
    HttpUtils.put(client, "http://localhost:9201/my_index",
      Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).mkString)
    client.close()

    ESClient.init()
    AsyncESClient.init()
  }

  after {
    runner.close()
    runner.clean()

    ESClient.shutdown()
    AsyncESClient.shutdown()
  }

  test("Insert with id"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true, true)

    client.insert(config, "123", Blog("Hello World!", "This is a first registration test!"))

    client.refresh(config)

    val result = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("123"))
    }

    assert(result == Some("123", Blog("Hello World!", "This is a first registration test!")))
  }

  test("Cluster health"){
    val client = ESClient("http://localhost:9201", true, true)

    assert(client.clusterHealth().get("cluster_name") == Some("elasticsearch-cluster-runner"))
  }

  test("Update partially"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true, true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data"))
    client.refresh(config)
    val registrationResult = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("1234"))
    }
    assert(registrationResult == Some("1234", Blog("Hello World!", "This is a registered data")))

    client.updatePartially(config, "1234", BlogContent("This is a updated data"))
    client.refresh(config)
    val updateResult1 = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("1234"))
    }
    assert(updateResult1 == Some("1234", Blog("Hello World!", "This is a updated data")))

    client.updatePartiallyJson(config, "1234", "{ \"subject\": \"Hello Scala!\" }")
    client.refresh(config)
    val updateResult2 = client.find[Blog](config){ builder =>
      builder.query(idsQuery("my_type").addIds("1234"))
    }
    assert(updateResult2 == Some("1234", Blog("Hello Scala!", "This is a updated data")))
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
    val client = ESClient("http://localhost:9201", true, true)

    // Register 100 docs
    (1 to 100).foreach { num =>
      client.insert(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
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

    // Delete 1 doc
//    client.delete(config, result1.get._1)
    client.deleteByQuery(config){ builder =>
      builder.query(matchPhraseQuery("subject", "10"))
    }
    client.refresh(config)

    // Check doc doesn't exist
    val result2 = client.find[Blog](config){ builder =>
      builder.query(matchPhraseQuery("subject", "10"))
    }
    assert(result2.isEmpty)

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

//    // Count by template
//    val count3 = client.countByTemplateAsInt(config)(
//      lang = "groovy",
//      template = "test_script",
//      params = Map("subjectValue" -> "Hello")
//    )
//    assert(count3 === 99)
  }

  test("noFields"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9201", true, true)

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
    assert(result.forall(_ == ()))
  }

  test("Async client"){
    val config = ESConfig("my_index", "my_type")
    val client = AsyncESClient("http://localhost:9201")

    val seqf = (1 to 100).map { num =>
      client.insertAsync(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
      ))
    }

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

  test("Async cluster health"){
    val config = ESConfig("my_index", "my_type")
    val client = AsyncESClient("http://localhost:9201")

    val result = client.clusterHealthAsync(config)

    val clusterHealth = Await.result(result, Duration.Inf)
    assert(clusterHealth.get("cluster_name") == Some("elasticsearch-cluster-runner"))
  }

}

object IntegrationTest {
  case class Blog(subject: String, content: String)
  case class BlogContent(content: String)
}