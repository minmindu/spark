name := "Simple Project"
version := "1.0"
organization := "com.bearhouse"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq(
		// spark dependency
		"org.apache.spark" % "spark-core_2.10" % "1.2.0" % "provided",
		// third party libraries
		"net.sf.jopt-simple" % "jopt-simple" % "4.3",
		"joda-time" % "joda-time" % "2.0"
		
)

// This statement includes the assembly plugin-in capabilities
// assemblySettings

// configure JAR used with the assembly plugin-in
 //jarName in assembly :="my-project-assembly.jar"

// A special option to exclude Scala itself from our assembly JAR, since Spark already bundlers Scala
//assemblyOption in assembly := 
//	(assemblyOption in assembly).value.copy(includeScala = false)