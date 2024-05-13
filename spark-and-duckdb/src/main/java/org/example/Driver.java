package org.example;

import org.example.iceberg.Reader;
import org.example.iceberg.Writer;

import java.sql.SQLException;

public class Driver
{
    public static String JDBC_URL = System.getenv("POSTGRES_JDBC_URL");

    public static void main(String[] args) throws SQLException {
        Writer writer = new Writer("test_jdbc_catalog", "default", "table1", JDBC_URL);
        writer.createTable();
        writer.populateTable();
        Reader reader = new Reader("test_jdbc_catalog", "default", "table1", JDBC_URL);
        reader.setup();
        reader.read();
        writer.dropTable();
        writer.close();
        reader.close();
    }
}
