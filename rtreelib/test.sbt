name := "rtree example"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "0.9.0-incubating"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.2.0"

libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "2.22"

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-actors" % _)

resolvers ++= Seq(
        "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
        "Akka Repository" at "http://repo.akka.io/releases/",
        "Spray Repository" at "http://repo.spray.cc/",
        "repo.codahale.com" at "http://repo.codahale.com",
        "Maven Central Server" at "http://repo1.maven.org/maven2",
        "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
        "artifactory" at "http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"
        )

ivyXML :=
<dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="2.5.0.v201103041518">
<artifact name="javax.servlet" type="orbit" ext="jar"/>
</dependency>

