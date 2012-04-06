name := "scaliak"

organization := "com.stackmob"

version := "0.1.1-SNAPSHOT"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1")

publishTo <<= (version) { version: String =>
  val stackmobNexus = "http://nexus/nexus/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at stackmobNexus + "snapshots/")
  else                                   Some("releases"  at stackmobNexus + "releases/")
}

resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases")

libraryDependencies ++= Seq(		          
    "org.scalaz" %% "scalaz-core" % "6.0.3" withSources(),
    "net.liftweb" %% "lift-json-scalaz" % "2.4-M5" withSources(),
    "com.basho.riak" % "riak-client" % "1.0.5" withSources(),
    "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources(),
    "org.specs2" %% "specs2" % "1.6.1" % "test" withSources(),
    "org.mockito" % "mockito-all" % "1.8.5" % "test" withSources(),
    "org.specs2" %% "specs2-scalaz-core" % "6.0.1" % "test" withSources())

logBuffered := false