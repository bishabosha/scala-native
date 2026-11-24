import java.util.Locale
import java.util.concurrent.TimeUnit

val isMac = System
  .getProperty("os.name", "unknown")
  .toLowerCase(Locale.ROOT)
  .startsWith("mac")

scalaVersion := {
  val scalaVersion = System.getProperty("scala.version")
  if (scalaVersion == null)
    throw new RuntimeException(
      """|The system property 'scala.version' is not defined.
         |Specify this property using the scriptedLaunchOpts -D.""".stripMargin
    )
  else scalaVersion
}

enablePlugins(ScalaNativePlugin)

// libdispatch runs callbacks on its own worker threads.
nativeConfig ~= {
  _.withMultithreading(true)
    .withLinkingOptions(
      _ ++ Seq("CoreFoundation", "CoreServices", "Network")
        .flatMap(Seq("-framework", _))
    )
}

def nativeExecutable(
    linkOutput: Any
)(implicit conv: xsbti.FileConverter): java.io.File =
  linkOutput match {
    case f: java.io.File           => f
    case ref: xsbti.VirtualFileRef => conv.toPath(ref).toFile()
  }

def runScenario(
    binary: java.io.File,
    scenario: String,
    timeoutSeconds: Int,
    expectedError: Option[String] = None
): Unit = {
  println(s"Running scenario: $scenario")
  val errors = java.io.File.createTempFile("libdispatch", ".err")
  val pb = new ProcessBuilder(binary.getAbsolutePath, scenario)
  pb.redirectOutput(ProcessBuilder.Redirect.INHERIT)
  pb.redirectError(errors)
  val proc = pb.start()
  if (!proc.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
    proc.destroyForcibly()
    sys.error(s"FAIL: $scenario did not complete in $timeoutSeconds seconds")
  }
  val stderr = IO.read(errors)
  errors.delete()
  System.err.print(stderr)
  val exitCode = proc.exitValue()
  expectedError match {
    case Some(message) =>
      if (exitCode == 0 || !stderr.contains(message))
        sys.error(s"FAIL: $scenario exited with $exitCode without: $message")
    case None =>
      if (exitCode != 0) sys.error(s"FAIL: $scenario exited with $exitCode")
  }
  println(s"$scenario PASSED")
}

val scenarios =
  Seq("queues", "idle", "main", "blocks", "fsevents", "network")

lazy val testAll = taskKey[Unit]("Run every libdispatch scenario")
testAll := Def.taskDyn {
  if (!isMac) Def.task(println("Skipping libdispatch tests, requires macOS"))
  else
    Def.task {
      implicit val conv: xsbti.FileConverter = Keys.fileConverter.value
      val binary = nativeExecutable((Compile / nativeLink).value)
      scenarios.foreach(runScenario(binary, _, timeoutSeconds = 120))
    }
}.value

lazy val testSingleThreaded =
  taskKey[Unit]("A single-threaded executable rejects foreign threads")
testSingleThreaded := Def.taskDyn {
  if (!isMac) Def.task(println("Skipping libdispatch tests, requires macOS"))
  else
    Def.task {
      implicit val conv: xsbti.FileConverter = Keys.fileConverter.value
      val binary = nativeExecutable((Compile / nativeLink).value)
      runScenario(
        binary,
        "queues",
        timeoutSeconds = 60,
        expectedError = Some("Enable multithreading")
      )
    }
}.value
