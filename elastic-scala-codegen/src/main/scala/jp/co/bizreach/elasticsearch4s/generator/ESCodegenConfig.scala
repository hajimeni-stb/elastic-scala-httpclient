package jp.co.bizreach.elasticsearch4s.generator

import java.nio.file.Paths
import Utils._

case class ESCodegenConfig(
  outputDir: Option[String],
  dateType: Option[String], // "joda" or "java8" (default is "joda")
  mappings: Seq[Mapping],
  typeMappings: Option[Map[String, String]]
)

case class Mapping(
  path: String,
  packageName: String,
  requiredProperties: Option[Seq[String]],
  arrayProperties: Option[Seq[String]],
  ignoreProperties: Option[Seq[String]],
  className: Option[String]
)

object ESCodegenConfig {
  def load(): ESCodegenConfig = {
    val json = read(Paths.get("es-codegen.json").toFile)
    parseJson[ESCodegenConfig](json)
  }
}