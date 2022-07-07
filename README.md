# Flink Table Store Trino

Because Trino's dependency is JDK 11, it is not possible to include the trino connector in [flink-table-store](https://github.com/apache/flink-table-store).

## Deploy Table Store Trino Connector

Building Table Store Trino Bundled Jar is by running:

- Trino 388: `mvn clean install -DskipTests`
- Trino 358: `mvn clean install -DskipTests -Ptrino-358`

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

## Query

```
SELECT * FROM tablestore.default.MyTable
```
