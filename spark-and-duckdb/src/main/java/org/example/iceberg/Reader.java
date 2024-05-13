package org.example.iceberg;

import org.example.iceberg.metadata.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

public class Reader
{
    Statement stmt;
    String catalog;
    String namespace;
    String jdbc;
    String table;

    public Reader(String catalog_name, String namespace, String tableName, String jdbc)
    {
        this.table = tableName;
        this.jdbc = jdbc;
        this.catalog = catalog_name;
        this.namespace = namespace;
    }

    public void setup()
    {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            this.stmt = conn.createStatement();
            stmt.execute("INSTALL iceberg");
            stmt.execute("LOAD iceberg");
            stmt.execute("INSTALL aws");
            stmt.execute("LOAD aws");
            printResultSet(stmt.executeQuery("CALL load_aws_credentials()"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void read() {
        try {
            Optional<String> metadata_location = Utils.getMetadataLocationForTableIfExists(jdbc, catalog, namespace, table);
            if (metadata_location.isPresent())
            {
                printResultSet(this.stmt.executeQuery(String.format("SELECT * FROM iceberg_scan('%s')", metadata_location.get())));
            }
            else
            {
                throw new IllegalStateException(String.format("No metadata location found for catalog %s namespace %s table %s", catalog, namespace, table));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        for (int i = 1; i <= columnsNumber; i++) {
            System.out.printf("%-50s", rsmd.getColumnName(i));
        }
        System.out.println();

        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                System.out.printf("%-50s", rs.getString(i));
            }
            System.out.println();
        }
    }

    public void close()
    {
        try {
            stmt.close();
        } catch (SQLException e) {
            System.out.println("FATAL: error closing statement");
        }
    }
}
