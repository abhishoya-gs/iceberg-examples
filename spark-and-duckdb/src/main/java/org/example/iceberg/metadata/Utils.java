package org.example.iceberg.metadata;

import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;

public class Utils
{
    public static Optional<String> getMetadataLocationForTableIfExists(String jdbc, String catalog, String namespace, String table)
    {
        Properties properties = new Properties();
        properties.put("user", System.getenv("POSTGRES_USER"));
        properties.put("password", System.getenv("POSTGRES_PASSWORD"));
        properties.put("ssl", false);
        try (Connection connection = new Driver().connect(jdbc, properties))
        {
            assert connection != null;
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT * FROM public.iceberg_tables\n" +
                            "WHERE catalog_name = '%s'\n" +
                            "AND table_namespace = '%s'\n" +
                            "AND table_name = '%s'\n", catalog, namespace, table));
            if (resultSet.next())
            {
                String metadataLocation = resultSet.getString("metadata_location");
                return Optional.of(metadataLocation);
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
        return Optional.empty();
    }
}
