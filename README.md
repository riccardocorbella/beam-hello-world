# beam-hello-world
This project contains an implementation of the classic big data hello world (e.g. word count) with Apache Beam.
To comipile and run the project you can use the commands:
```shell
mvn compile
mvn exec:java -Dexec.mainClass="WordCount" -Dexec.args="<input_file> <output_prefix>"
```
or:
```shell
mvn package
java -jar beam-hello-world-0.1-jar-with-dependencies.jar <input_file> <output_prefix>
```
