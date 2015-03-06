name := "RxKinesis"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.6"

logLevel := Level.Warn

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.2.1",
  "io.reactivex" % "rxscala_2.11" % "0.23.1",
  "com.amazonaws" % "aws-java-sdk" % "1.9.19",
  "log4j" % "log4j" % "1.2.17",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)
    