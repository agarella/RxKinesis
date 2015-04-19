name := "TwitterAnalytics"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "default" % "rxkinesis_2.11" % "0.0.1-SNAPSHOT",
  "io.spray" %%  "spray-json" % "1.3.1",
  "com.twitter" % "hbc-core" % "2.2.0",
  "org.slf4j" % "slf4j-nop" % "1.7.12"
)