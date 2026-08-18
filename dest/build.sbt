scalaVersion := "3.8.3"

run / javaOptions ++= Seq(
  "--add-exports",
  "java.base/jdk.internal.vm=ALL-UNNAMED",
)

resolvers ++= Seq("repo-0" at "file:///nix/store/4icsqq77yz6q0k2rmnz6shng26mbim3z-scala-next-3.8.4/maven2", "repo-1" at "file:///nix/store/4icsqq77yz6q0k2rmnz6shng26mbim3z-scala-next-3.8.4/maven2")

libraryDependencies ++= Seq(
  "dev.hnaderi" %% "scala-k8s-http4s" % "0.31.0" ,
  "dev.hnaderi" %% "scala-k8s-http4s-ember" % "0.31.0" ,
  "dev.hnaderi" %% "scala-k8s-circe" % "0.31.0" ,
  "org.http4s" %% "http4s-circe" % "0.23.36" ,
  "org.typelevel" %% "cats-effect" % "3.7.0" ,
  "org.typelevel" %% "cats-effect-direct" % "1.0.0" 
)

libraryDependencies ++= Seq(
  "dev.hnaderi" %% "scala-k8s-http4s" % "0.31.0" % Test,
  "dev.hnaderi" %% "scala-k8s-http4s-ember" % "0.31.0" % Test,
  "dev.hnaderi" %% "scala-k8s-circe" % "0.31.0" % Test,
  "org.http4s" %% "http4s-circe" % "0.23.36" % Test,
  "org.typelevel" %% "cats-effect" % "3.7.0" % Test,
  "org.typelevel" %% "cats-effect-direct" % "1.0.0" % Test
)

