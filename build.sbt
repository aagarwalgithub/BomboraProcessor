name := "BomboraProcessor"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions += "-target:jvm-1.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.adroll.slargma" % "enigma" % "0.0.1-SNAPSHOT",
  "com.adroll.slargma" % "segments" % "0.0.1-SNAPSHOT"
)
resolvers += "jitpack" at "https://jitpack.io"
resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "myjar.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//sbt clean assembly