package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import org.apache.calcite.jdbc.CalciteSchema;

public class App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <arg1> <arg2>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as you want.
        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tArg1: " + arg1);
        String arg2 = args[1];
        System.out.println("\tArg2: " + arg2);

        // Note: in practice, you would probably use
        // org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure
        // Calcite.
        // But there's a lot of magic happening there; since this is an educational
        // project,
        // we guide you towards the explicit method in the writeup.

        // Connect to DuckDB

        Set<String> badQueries = new HashSet<>(
                Arrays.asList("q16.sql", "q19.sql", "q9.sql", "q10.sql", "q21.sql"));

        Optimizer optimizer = new Optimizer(args[0]);
        // Iterate over target queries
        File queryDir = new File(args[0]);
        for (File file : queryDir.listFiles()) {
            if (file.getName().endsWith(".sql")) {
                // if (file.getName().equals("capybara4.sql")) {
                System.out.println("Optimizing query: " + file.getName());
                String optimizedQuery = optimizer.optimize(file.getPath(), args[1],
                        !badQueries.contains(file.getName()));
            }
        }
    }
}
