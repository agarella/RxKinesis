name := "RxKinesis"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.2.1",
  "io.reactivex" % "rxscala_2.11" % "0.23.1",
  "com.amazonaws" % "aws-java-sdk" % "1.9.19",
  "org.scalatest" % "scalatest_2.11" % "2.2.4",
  "org.mockito" % "mockito-all" % "1.10.19"
)
    