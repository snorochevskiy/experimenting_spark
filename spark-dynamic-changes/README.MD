# Runtime Spark changes

This small project demonstrates how some functionality can be changed in the runtime for Spark structured streaming app.

## Launching 

In a separate console run net cat server.

```
nc -lk 9999
```

Run app on local spark cluster:

```
$SPARK_HOME/bin/spark-submit ~/dev/proj/spark-dynamic-changes/target/scala-2.11/dynamic-changes-assembly-0.1.0-SNAPSHOT.jar
```

Then, manually post to the console where net cut is running: `{"counter": 1}`.
The reaction should be printed in spark app console as:

```
+-------+
|counter|
+-------+
|      5|
+-------+
```

After ~ 10 seconds post same `{"counter": 1}` again. Spark app console output should change.

```
+-------+
|counter|
+-------+
|      9|
+-------+
```

