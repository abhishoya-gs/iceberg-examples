# Iceberg Example using Spark and DuckDb

## Code structure

`org.example.Driver` - main driver code to run a job which creates a writer and reader node, does a basic create table, insert, read and drop.

`org.example.iceberg.Writer` - writer implemented using spark to write data in S3 and metadata in Postgres

`org.example.icerberg.Reader` - reader implemented using duck db to read data from S3 and metadata from Postgres

## Pre-requisites

- Java 11+ & Maven for building code
- Postgres for metadata
- S3 Bucket for data

## Environment Variables

```env
AWS_ACCESS_KEY_ID=XXXXX;
AWS_REGION=aws-region-1;
AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXX;
POSTGRES_JDBC_URL=jdbc:postgresql://host:port/db;
POSTGRES_USER=someuser;
POSTGRES_PASSWORD=somepassword;
S3_URI=s3://sample-bucket/warehouse
```