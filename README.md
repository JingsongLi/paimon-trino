# Paimon Trino

Because Trino's dependency is JDK 11, it is not possible to include the trino connector in [Paimon](https://github.com/apache/incubator-paimon).

## Deploy Paimon Trino Connector

Building Paimon Trino Bundled Jar is by running:

- Trino 388: `mvn clean install -DskipTests` , (JDK 11 required).
- Trino 358: `mvn clean install -DskipTests -Ptrino-358` , (JDK 11 required).
- Trino 391: `mvn clean install -DskipTests -Ptrino-391` , (JDK 17 required).
- Trino 391+: You can change `target.java.version` to `17`, and `trino.version` to trino version, and `mvn clean install -DskipTests`. Use JDK 17 and add jvm options like trino 391.

NOTE: For JDK 17,  when [Deploying Trino](https://trino.io/docs/current/installation/deployment.html), should add jvm options: `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`

Then, copy `target/paimon-trino-*.jar` and [flink-shaded-hadoop-2-uber-2.8.3-10.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)
to `plugin/paimon`.

## Configure Paimon Catalog

Catalogs are registered by creating a catalog properties file in the etc/catalog directory.
For example, create `etc/catalog/paimon.properties` with the following contents to mount
the paimon connector as the paimon catalog:

```
connector.name=paimon
warehouse=file:/tmp/warehouse
```

If you are using HDFS, choose one of the following ways to configure your HDFS:

- set environment variable `HADOOP_HOME`.
- set environment variable `HADOOP_CONF_DIR`.
- configure `hadoop-conf-dir` in the properties.

## Query

```
SELECT * FROM paimon.default.MyTable
```

## Query with Time Traveling

```
SET SESSION paimon.scan_timestamp_millis=1679486589444;
SELECT * FROM paimon.default.MyTable;
```
