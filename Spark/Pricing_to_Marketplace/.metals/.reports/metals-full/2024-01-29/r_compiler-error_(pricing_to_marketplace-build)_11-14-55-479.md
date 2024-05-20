file://<WORKSPACE>/build.sbt
### file%3A%2F%2F%2FUsers%2Fricardomaravilla%2FDocuments%2FGitHub%2FStreaming%2FPricing_to_Marketplace%2Fbuild.sbt:34: error: unclosed string literal
    "com.ibm.cloud" % "cloudant" % "0.8.1,
                                   ^

occurred in the presentation compiler.

action parameters:
uri: file://<WORKSPACE>/build.sbt
text:
```scala
import _root_.scala.xml.{TopScope=>$scope}
import _root_.sbt._
import _root_.sbt.Keys._
import _root_.sbt.nio.Keys._
import _root_.sbt.ScriptedPlugin.autoImport._, _root_.sbt.plugins.JUnitXmlReportPlugin.autoImport._, _root_.sbt.plugins.MiniDependencyTreePlugin.autoImport._, _root_.bloop.integrations.sbt.BloopPlugin.autoImport._, _root_.scalafix.sbt.ScalafixPlugin.autoImport._, _root_.scalafix.sbt.ScalafixTestkitPlugin.autoImport._, _root_.sbtbuildinfo.BuildInfoPlugin.autoImport._, _root_.net.vonbuchholtz.sbt.dependencycheck.DependencyCheckPlugin.autoImport._, _root_.xerial.sbt.pack.PackPlugin.autoImport._
import _root_.sbt.plugins.IvyPlugin, _root_.sbt.plugins.JvmPlugin, _root_.sbt.plugins.CorePlugin, _root_.sbt.ScriptedPlugin, _root_.sbt.plugins.SbtPlugin, _root_.sbt.plugins.SemanticdbPlugin, _root_.sbt.plugins.JUnitXmlReportPlugin, _root_.sbt.plugins.Giter8TemplatePlugin, _root_.sbt.plugins.MiniDependencyTreePlugin, _root_.bloop.integrations.sbt.BloopPlugin, _root_.scalafix.sbt.ScalafixPlugin, _root_.scalafix.sbt.ScalafixTestkitPlugin, _root_.sbtbuildinfo.BuildInfoPlugin, _root_.net.vonbuchholtz.sbt.dependencycheck.DependencyCheckPlugin, _root_.xerial.sbt.pack.PackPlugin
/*
Name: build.sbt
Description: Build definition
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2022/01/19
Modification:
    date        owner                           description
*/

name := "Streaming Pricing to Marketplace"
version := "2.0.0"
scalaVersion := "2.13.12"
//scalaVersion := "2.12.15"
enablePlugins(PackPlugin)
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.0-dswdia-SNAPSHOT" ,
    "org.apache.spark" %% "spark-core" % "4.0.0-dswdia-SNAPSHOT" % "provided",
    "org.apache.spark" %% "spark-sql" % "4.0.0-dswdia-SNAPSHOT" % "provided",
    "org.apache.kafka" %% "kafka" % "3.6.1",
    "org.apache.httpcomponents" % "httpclient" % "4.5.13" % "provided",
    "org.apache.spark" %% "spark-avro" % "3.3.0",
	"za.co.absa" %% "abris" % "6.3.0",
    "redis.clients" % "jedis" % "4.2.3",
    "com.ibm.db2" % "jcc" % "11.5.9.0",
    "org.scalactic" %% "scalactic" % "3.2.10",
    "com.ibm.cloud" % "cloudant" % "0.8.1,
    "org.scalatest" %% "scalatest" % "3.2.10" % "test",
    "com.bettercloud" % "vault-java-driver" % "5.1.0" % "provided",
    "com.ibm.dswdia" %% "core" % "3.0.2",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3",
    //"com.lihaoyi" %% "upickle" % "0.7.1",
    "com.alibaba.fastjson2" % "fastjson2" % "2.0.23",
    "joda-time" % "joda-time" % "2.3",
    "com.lihaoyi" %% "requests" % "0.6.9",
    "com.lihaoyi" %% "ujson" % "1.4.2"
).map(_.exclude("org.apache.hadoop", "hadoop-client.*"))
 .map(_.exclude("io.netty", "netty-all.*"))
 .map(_.exclude("org.jetbrains.kotlin", "kotlin-stdlib.*"))
 //.map(_.exclude("com.fasterxml.jackson.core", "jackson-databind.*"))

dependencyOverrides ++= Seq(
    "com.google.code.gson" % "gson" % "2.8.9",
    "org.apache.hadoop" % "hadoop-client" % "3.3.3",
    "io.netty" % "netty-all" % "4.1.77.Final",
    "org.jetbrains.kotlin" % "kotlin-stdlib" % "1.6.0",
    //"com.fasterxml.jackson.core" % "jackson-databind" % "2.13.4",
)

resolvers ++= Seq(
    "confluent" at "https://packages.confluent.io/maven/",
    "mvnrepository" at "https://mvnrepository.com/",
    "Artima Maven Repository" at "https://repo.artima.com/releases",
    "Artifactory" at "https://na.artifactory.swg-devops.com/artifactory/txo-dswim-esb-sbt-local/"
)
```



#### Error stacktrace:

```
scala.meta.internal.tokenizers.Reporter.syntaxError(Reporter.scala:23)
	scala.meta.internal.tokenizers.Reporter.syntaxError$(Reporter.scala:23)
	scala.meta.internal.tokenizers.Reporter$$anon$1.syntaxError(Reporter.scala:33)
	scala.meta.internal.tokenizers.Reporter.syntaxError(Reporter.scala:25)
	scala.meta.internal.tokenizers.Reporter.syntaxError$(Reporter.scala:25)
	scala.meta.internal.tokenizers.Reporter$$anon$1.syntaxError(Reporter.scala:33)
	scala.meta.internal.tokenizers.LegacyScanner.getStringLit(LegacyScanner.scala:553)
	scala.meta.internal.tokenizers.LegacyScanner.fetchDoubleQuote$1(LegacyScanner.scala:372)
	scala.meta.internal.tokenizers.LegacyScanner.fetchToken(LegacyScanner.scala:376)
	scala.meta.internal.tokenizers.LegacyScanner.nextToken(LegacyScanner.scala:211)
	scala.meta.internal.tokenizers.LegacyScanner.foreach(LegacyScanner.scala:1011)
	scala.meta.internal.tokenizers.ScalametaTokenizer.uncachedTokenize(ScalametaTokenizer.scala:24)
	scala.meta.internal.tokenizers.ScalametaTokenizer.$anonfun$tokenize$1(ScalametaTokenizer.scala:17)
	scala.collection.concurrent.TrieMap.getOrElseUpdate(TrieMap.scala:895)
	scala.meta.internal.tokenizers.ScalametaTokenizer.tokenize(ScalametaTokenizer.scala:17)
	scala.meta.internal.tokenizers.ScalametaTokenizer$$anon$2.apply(ScalametaTokenizer.scala:332)
	scala.meta.tokenizers.Api$XtensionTokenizeDialectInput.tokenize(Api.scala:25)
	scala.meta.tokenizers.Api$XtensionTokenizeInputLike.tokenize(Api.scala:14)
	scala.meta.internal.parsers.ScannerTokens$.apply(ScannerTokens.scala:914)
	scala.meta.internal.parsers.ScalametaParser.<init>(ScalametaParser.scala:33)
	scala.meta.parsers.Parse$$anon$1.apply(Parse.scala:35)
	scala.meta.parsers.Api$XtensionParseDialectInput.parse(Api.scala:25)
	scala.meta.internal.semanticdb.scalac.ParseOps$XtensionCompilationUnitSource.toSource(ParseOps.scala:17)
	scala.meta.internal.semanticdb.scalac.TextDocumentOps$XtensionCompilationUnitDocument.toTextDocument(TextDocumentOps.scala:206)
	scala.meta.internal.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:54)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$semanticdbTextDocument$1(ScalaPresentationCompiler.scala:374)
```
#### Short summary: 

file%3A%2F%2F%2FUsers%2Fricardomaravilla%2FDocuments%2FGitHub%2FStreaming%2FPricing_to_Marketplace%2Fbuild.sbt:34: error: unclosed string literal
    "com.ibm.cloud" % "cloudant" % "0.8.1,
                                   ^