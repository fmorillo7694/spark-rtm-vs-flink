// Scala job for Spark 4.1.2 Real-time Mode (RTM).
// Spark + Kafka connector are "provided": they ship with the cluster image and the
// spark-submit --packages flag, so the assembly jar stays tiny (just our pipeline).

ThisBuild / scalaVersion := "2.13.16"

lazy val sparkVersion = "4.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "scala-rtm",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core"  % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
    ),
    // We don't shade anything (all deps provided) — a plain package jar is enough.
    Compile / mainClass := Some("RtmPipeline")
  )
