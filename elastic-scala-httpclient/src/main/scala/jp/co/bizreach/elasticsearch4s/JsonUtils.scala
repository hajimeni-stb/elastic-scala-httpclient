package jp.co.bizreach.elasticsearch4s

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneOffset}

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, Version}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.reflect.ClassTag

private[elasticsearch4s] object JsonUtils {

  private val mapper = new ObjectMapper()
  mapper.enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new SimpleModule("MyModule", Version.unknownVersion())
    .addSerializer(classOf[DateTime], new JsonSerializer[DateTime](){
      override def serialize(value: DateTime, generator: JsonGenerator, serializers: SerializerProvider): Unit = {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
        generator.writeString(formatter.print(value))
      }
    })
    .addDeserializer(classOf[DateTime], new JsonDeserializer[DateTime]{
      override def deserialize(parser: JsonParser, context: DeserializationContext): DateTime = {
          val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
          formatter.parseDateTime(if(parser.getValueAsString != null) parser.getValueAsString else parser.nextTextValue)
      }
    })
    .addSerializer(classOf[OffsetDateTime], new JsonSerializer[OffsetDateTime]{
      override def serialize(value: OffsetDateTime, generator: JsonGenerator, serializers: SerializerProvider): Unit = {
        val formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC)
        generator.writeString(formatter.format(value))
      }
    })
    .addDeserializer(classOf[OffsetDateTime], new JsonDeserializer[OffsetDateTime]{
      override def deserialize(parser: JsonParser, context: DeserializationContext): OffsetDateTime = {
        OffsetDateTime.parse(
          if(parser.getValueAsString != null) parser.getValueAsString else parser.nextTextValue,
          DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC)
        )
      }
    })
  )

  def serialize(doc: AnyRef): String = mapper.writeValueAsString(doc)

  def deserialize[T](json: String)(implicit c: ClassTag[T]): T = {
    if(c.runtimeClass == classOf[Unit]){
      ().asInstanceOf[T]
    } else {
      mapper.readValue(json, c.runtimeClass).asInstanceOf[T]
    }
  }

}
