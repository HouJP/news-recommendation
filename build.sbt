name := "NewsRecommendation"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark"    %%  "spark-core"    % "1.4.0",
  "org.apache.spark"    %%  "spark-mllib"   % "1.4.0",
  "org.apache.spark"    %%  "spark-sql"     % "1.4.0",
  "org.scalatest"       %%  "scalatest"     % "2.2.1"   % "test",
  "com.databricks"      %%  "spark-csv"     % "1.0.3",
  "com.github.nscala-time" %% "nscala-time" % "2.0.0",
  "io.spray"            %%  "spray-can"     % "1.3.3",
  "io.spray"            %%  "spray-routing" % "1.3.3",
  "io.spray"            %%  "spray-testkit" % "1.3.3"   % "test",
  "com.typesafe.akka"   %%  "akka-actor"    % "2.3.9",
  "com.typesafe.akka"   %%  "akka-testkit"  % "2.3.9"   % "test",
  "org.specs2"          %%  "specs2-core"   % "2.3.11"  % "test",
  "org.json4s"          %%  "json4s-native" % "3.2.11",
  "com.github.scopt"    %%  "scopt"         % "3.3.0"
)

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += Resolver.sonatypeRepo("public")

assemblyMergeStrategy in assembly := {
  case PathList("akka", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", "common", "base", xs @_*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Log.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}