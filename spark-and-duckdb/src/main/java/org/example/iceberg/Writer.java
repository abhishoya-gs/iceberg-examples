package org.example.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

public class Writer
{
    SparkSession sparkSession;
    String prefix;
    String jdbc;
    String tableName;

    public Writer(String catalog_name, String namespace, String tableName, String jdbc)
    {
        this.prefix = "spark.sql.catalog." + catalog_name;
        this.sparkSession = SparkSession.builder()
                .config("spark.master", "local")
                .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getCanonicalName())
                .config("spark.sql.defaultCatalog", catalog_name)
                .config(prefix, SparkCatalog.class.getCanonicalName())
                .config(getKey("io-impl"), S3FileIO.class.getCanonicalName())
                .config(getKey("warehouse"), System.getenv("S3_URI"))
                .config(getKey("catalog-impl"), JdbcCatalog.class.getCanonicalName())
                .config(getKey("uri"), jdbc)
                .config(getKey("jdbc.user"), System.getenv("POSTGRES_USER"))
                .config(getKey("jdbc.password"), System.getenv("POSTGRES_PASSWORD"))
                .config(getKey("default-namespace"), namespace)
                .config("spark.sql.catalogImplementation", "in-memory")
                .getOrCreate();
        this.tableName = tableName;
    }

    @NotNull
    private String getKey(String key) {
        return prefix + "." + key;
    }

    public void createTable()
    {
        sparkSession.sql(String.format("CREATE TABLE IF NOT EXISTS %s (\n" +
                "    id int NOT NULL COMMENT 'unique id',\n" +
                "    name string,\n" +
                "    role string\n" +
                ") PARTITIONED BY (role)", tableName));
    }

    public void populateTable()
    {
        sparkSession.sql(String.format("insert into %s(id, name, role) values(1,'Abhishoya','admin')", tableName));
    }

    public void dropTable()
    {
        sparkSession.sql(String.format("DROP TABLE %s", tableName));
    }

    public void close()
    {
        sparkSession.close();
    }
}
