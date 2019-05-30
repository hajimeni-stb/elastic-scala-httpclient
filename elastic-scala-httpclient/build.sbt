name := "elastic-scala-httpclient"

organization := "jp.co.bizreach"

version := "4.0.0"

scalaVersion := "2.12.8"

crossScalaVersions := Seq("2.11.12", scalaVersion.value)

libraryDependencies ++= Seq(
  "org.slf4j"                    %  "slf4j-api"            % "1.7.26",
  "org.asynchttpclient"          %  "async-http-client"    % "2.9.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
  "org.elasticsearch"            %  "elasticsearch"        % "6.7.2",
  "org.codelibs"                 %  "elasticsearch-cluster-runner" % "6.7.2.0" % "test",
  "org.codelibs"                 %  "elasticsearch-sstmpl"         % "6.7.0"   % "test",
  "org.scalatest"                %% "scalatest"                    % "3.0.7"   % "test"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalacOptions := Seq("-deprecation")

javacOptions in compile ++= Seq("-source","1.8", "-target","1.8", "-encoding","UTF-8")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/bizreach/elastic-scala-httpclient</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/bizreach/elastic-scala-httpclient</url>
      <connection>scm:git:https://github.com/bizreach/elastic-scala-httpclient.git</connection>
    </scm>
    <developers>
      <developer>
        <id>takezoe</id>
        <name>Naoki Takezoe</name>
        <email>naoki.takezoe_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
      <developer>
        <id>hajimeni</id>
        <name>Hajime Nishiyama</name>
        <email>nishiyama_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
      <developer>
        <id>saito400</id>
        <name>Kenichi Saito</name>
        <email>kenichi.saito_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
      <developer>
        <id>shimamoto</id>
        <name>Takako Shimamoto</name>
        <email>takako.shimamoto_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
    </developers>)
