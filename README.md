# ReAGEnT-Spark
This part is responsible for taking the raw information from the Mongo DB and computing the information for the frontend. Thereafter saving it again in the Mongo DB.

## Build

To create an executable jar bundled with all dependencies, we use sbt-assembly. It is defined at project/plugins.sbt.

To build a .jar file, run sbt clean assembly in the project directory. The generated jar will be located in target/scala-<SCALA_VERSION>/ReAGEnT-Spark-assembly-0.1.jar.

## Local startup

Execute the jar with java -jar ReAGEnT-Spark-assembly-0.1.jar
