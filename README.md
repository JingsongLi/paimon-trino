# Flink Table Store Trino

Because Trino's dependency is JDK 11, it is not possible to include the trino connector in https://github.com/apache/flink-table-store .

## Deploy Table Store Trino Connector

Building Table Store Trino Bundled Jar is by running:

```
mvn clean install -DskipTests
```

Then, deploy bundled jar to Trino cluster:
https://trino.io/docs/current/develop/spi-overview.html#deploying-a-custom-plugin

## Configure Table Store Catalog

Catalogs are registered by creating a catalog properties file in the etc/catalog directory.
For example, create etc/catalog/tablestore.properties with the following contents to mount
the tablestore connector as the tablestore catalog:

```
connector.name=tablestore
warehouse=file:/tmp/warehouse
```

## Query

```
SELECT * FROM tablestore.default.MyTable
```
