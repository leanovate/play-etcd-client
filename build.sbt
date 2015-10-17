import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

lazy val root = (project in file(".")).configs(IntegrationTest).settings(Defaults.itSettings: _*)

name := "play-etcd-client"

organization := "de.leanovate"

scalaVersion := "2.11.7"

scalacOptions := Seq("-deprecation")

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-ws" % "2.4.3" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test, it",
  "org.mockito" % "mockito-core" % "1.10.19" % "test",
  "de.leanovate.play-mockws" %% "play-mockws" % "2.4.1" % "test",
  "com.typesafe.play" %% "play-test" % "2.4.3" % "test, it"
)

fork in run := true

publishMavenStyle := true

pomExtra := {
  <url>https://github.com/leanovate/play-etcd-client</url>
    <licenses>
      <license>
        <name>MIT</name>
        <url>http://opensource.org/licenses/MIT</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/leanovate/play-etcd-client</connection>
      <developerConnection>scm:git:git@github.com:/leanovate/play-etcd-client</developerConnection>
      <url>github.com/leanovate/play-etcd-client</url>
    </scm>
    <developers>
      <developer>
        <id>untoldwind</id>
        <name>Bodo Junglas</name>
        <url>http://untoldwind.github.io/</url>
      </developer>
    </developers>
}

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _)),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
  pushChanges
)
