organization := "com.lightbend.lagom"

name := "lagom-service-locator-zookeeper"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.8"

val lagomVersion = "1.0.0-RC1"

libraryDependencies ++= Seq(
  "com.lightbend.lagom" %% "lagom-javadsl-api"   % lagomVersion,
  "org.apache.curator"   % "curator-x-discovery" % "2.11.0",
  "org.apache.curator"   % "curator-test"        % "2.11.0" % Test,
  "org.scalatest"       %% "scalatest"           % "2.2.4" % Test,
  "com.typesafe.akka" 	%% "akka-testkit" 		% "2.4.4" % Test,
  "io.spray" 			%% "spray-json" 		% "1.3.2"
)