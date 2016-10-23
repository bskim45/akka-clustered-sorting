lazy val commonSettings = Seq(
  name := "Master",
  version := "1.0",
  organization := "com.yellow",
  scalaVersion := "2.11.7",
  resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  resolvers += Resolver.sonatypeRepo("public"),
  libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.3.14",
    "com.typesafe.akka" %% "akka-remote" % "2.3.14"
  ),
  javaOptions in run ++= Seq(
    "-Xms256M", "-Xmx24G","-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC"),
  javaOptions in Universal ++= Seq(
    // -J params will be added as jvm parameters
    "-J-Xms256M", "-J-Xmx24G","-J-XX:+UseConcMarkSweepGC", "-J-XX:+UseParNewGC")
)

lazy val Common = RootProject(file("../Common"))

lazy val Master = (project in file(".")).
  settings(commonSettings).
  dependsOn(Common).
  enablePlugins(JavaAppPackaging)

assemblyJarName in assembly := "master.jar"

// removes all jar mappings in universal and appends the fat jar
mappings in Universal <<= (mappings in Universal, assembly in Compile) map { (mappings, fatJar) =>
  val filtered = mappings filter { case (file, name) =>  ! name.endsWith(".jar") }
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

// the bash scripts classpath only needs the fat jar
scriptClasspath := Seq( (assemblyJarName in assembly).value )