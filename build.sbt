name := "RxKinesis"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.2.1",
  "io.reactivex" % "rxscala_2.11" % "0.24.0",
  "com.amazonaws" % "aws-java-sdk" % "1.9.26",
  "log4j" % "log4j" % "1.2.17",
  "joda-time" % "joda-time" % "2.7",
  "org.joda" % "joda-convert" % "1.7",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)
    