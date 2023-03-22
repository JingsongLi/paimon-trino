# Flink Table Store Trino

Because Trino's dependency is JDK 11, it is not possible to include the trino connector in [flink-table-store](https://github.com/apache/flink-table-store).

## Deploy Table Store Trino Connector

Building Table Store Trino Bundled Jar is by running:

- Trino 388: `mvn clean install -DskipTests` , (JDK 11 required).
- Trino 358: `mvn clean install -DskipTests -Ptrino-358` , (JDK 11 required).
- Trino 391: `mvn clean install -DskipTests -Ptrino-391` , (JDK 17 required).
- Trino 391+: You can change `target.java.version` to `17`, and `trino.version` to trino version, and `mvn clean install -DskipTests`. Use JDK 17 and add jvm options like trino 391.

NOTE: For JDK 17,  when [Deploying Trino](https://trino.io/docs/current/installation/deployment.html), should add jvm options: `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`

Then, copy `target/flink-table-store-trino-*.jar` and [flink-shaded-hadoop-2-uber-2.8.3-10.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)
to `plugin/tablestore`.

## Configure Table Store Catalog

Catalogs are registered by creating a catalog properties file in the etc/catalog directory.
For example, create `etc/catalog/tablestore.properties` with the following contents to mount
the tablestore connector as the tablestore catalog:

```
connector.name=tablestore
warehouse=file:/tmp/warehouse
```

If you are using HDFS, choose one of the following ways to configure your HDFS:

- set environment variable `HADOOP_HOME`.
- set environment variable `HADOOP_CONF_DIR`.
- configure `fs.hdfs.hadoopconf` in the properties.

## Security

You can configure kerberos keytag file when using KERBEROS authentication in the properties.
```
security.kerberos.login.principal=hadoop-user
security.kerberos.login.keytab=/etc/trino/hdfs.keytab
```
Keytab files must be distributed to every node in the cluster that runs Trino.

## Query

```
SELECT * FROM tablestore.default.MyTable
```

## Query With Time Traveling

Time Traveling is not supported in 0.3.
Please use >= 0.4 versions.