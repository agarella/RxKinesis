name := "RxKinesis"

version := "1.0"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "io.reactivex" % "rxscala_2.11" % "0.23.1",
  "com.amazonaws" % "aws-java-sdk" % "1.9.19",
  "org.scalatest" % "scalatest_2.11" % "2.2.4",
  "org.mockito" % "mockito-all" % "1.10.19"
)
    