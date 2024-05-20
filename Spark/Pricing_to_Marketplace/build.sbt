/*
Name: build.sbt
Description: Build definition
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2022/01/19
Modification:
    date        owner                           description
*/

name := "pricing-to-marketplace"
version := "2.0.5"
scalaVersion := "2.13.12"
//scalaVersion := "2.12.15"
organization := "com.ibm.dswdia"
//enablePlugins(PackPlugin)
//credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val root = (project in file(".")).
enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "App",
  )

enablePlugins(PackPlugin)


libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.0-dswdia-SNAPSHOT" ,
    "org.apache.spark" %% "spark-core" % "4.0.0-dswdia-SNAPSHOT" % "provided",
    "org.apache.spark" %% "spark-sql" % "4.0.0-dswdia-SNAPSHOT" % "provided",
    "org.apache.spark" %% "spark-avro" % "3.5.0",
    "org.apache.kafka" %% "kafka" % "3.6.1",
    "org.apache.httpcomponents" % "httpclient" % "4.5.14" % "provided",
	"za.co.absa" %% "abris" % "6.3.0",
    "redis.clients" % "jedis" % "5.1.0",
    "com.ibm.db2" % "jcc" % "11.5.9.0",
    "org.scalactic" %% "scalactic" % "3.2.17",
    "com.ibm.cloud" % "cloudant" % "0.8.1",
    "org.scalatest" %% "scalatest" % "3.2.17" % "test",
    "com.bettercloud" % "vault-java-driver" % "5.1.0" % "provided",
    "com.ibm.dswdia" %% "core" % "3.0.3",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.16.0",
    //"com.lihaoyi" %% "upickle" % "0.7.1",
    "com.alibaba.fastjson2" % "fastjson2" % "2.0.45",
    "joda-time" % "joda-time" % "2.12.6",
    "com.lihaoyi" %% "requests" % "0.8.0",
    "com.lihaoyi" %% "ujson" % "3.1.4"
).map(_.exclude("org.apache.hadoop", "hadoop-client.*"))
 .map(_.exclude("io.netty", "netty-all.*"))
 .map(_.exclude("org.jetbrains.kotlin", "kotlin-stdlib.*"))
 //.map(_.exclude("com.fasterxml.jackson.core", "jackson-databind.*"))

dependencyOverrides ++= Seq(
    "com.google.code.gson" % "gson" % "2.10.1",
    "org.apache.hadoop" % "hadoop-client" % "3.3.6",
    "io.netty" % "netty-all" % "4.1.106.Final",
    "org.jetbrains.kotlin" % "kotlin-stdlib" % "1.9.22",
    //"com.fasterxml.jackson.core" % "jackson-databind" % "2.13.4",
)

resolvers ++= Seq(
    "Artifactory third party" at "https://na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-thirdparty-maven-local/",
    "Artifactory" at "https://na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-sbt-local/",
    "confluent" at "https://packages.confluent.io/maven/",
    "mvnrepository" at "https://mvnrepository.com/",
    "Artima Maven Repository" at "https://repo.artima.com/releases"
)