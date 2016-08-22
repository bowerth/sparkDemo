# sparkDemo

## run interactively

```
$ cd sparkDemo
$ activator
> runMain CompleteTfCpc
```

## run in batch mode

### package for submission

```
$ cd sparkDemo
$ activator
> clean
> compile
> package
```

### run the application using spark-submit

```
$SPARK_HOME/bin/spark-submit \
--class "SimpleApp" \
--master local[4] \
target/scala-2.11/sparkdemo_2.11-1.0-SNAPSHOT.jar
```
