package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.rel.RelNode;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Runner {
    public static void execute(RelNode query, CalciteSchema schema) {
        try {

            // Create a RelBuilder
            RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder()
                    .defaultSchema(schema.plus())
                    .build());

            // Build a RelNode (example query)
            RelNode relNode = builder
                    .scan("my_table")
                    .filter(builder.equals(builder.field("column_name"), builder.literal("value")))
                    .build();

            // Use RelRunners to execute the RelNode
            PreparedStatement preparedStatement = RelRunners.run(relNode);
            ResultSet resultSet = preparedStatement.executeQuery();

            // Process the result set
            while (resultSet.next()) {
                System.out.println(resultSet.getString("column_name"));
            }

            // Close resources
            resultSet.close();
            preparedStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}