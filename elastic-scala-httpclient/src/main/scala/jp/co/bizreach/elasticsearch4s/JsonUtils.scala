package jp.co.bizreach.elasticsearch4s

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneOffset, ZonedDateTime}

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
    .addSerializer(classOf[DateTime], (value: DateTime, generator: JsonGenerator, provider: SerializerProvider) => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
      generator.writeString(formatter.print(value))
    })
    .addDeserializer(classOf[DateTime], (parser: JsonParser, context: DeserializationContext) => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
      formatter.parseDateTime(if(parser.getValueAsString != null) parser.getValueAsString else parser.nextTextValue)
    })
    .addSerializer(classOf[OffsetDateTime], (value: OffsetDateTime, generator: JsonGenerator, provider: SerializerProvider) => {
      val formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC)
      generator.writeString(formatter.format(value))
    })
    .addDeserializer(classOf[OffsetDateTime], (parser: JsonParser, context: DeserializationContext) => {
      OffsetDateTime.parse(
        if(parser.getValueAsString != null) parser.getValueAsString else parser.nextTextValue,
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC)
      )
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
