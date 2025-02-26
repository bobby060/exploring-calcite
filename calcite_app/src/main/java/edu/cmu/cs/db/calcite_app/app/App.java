package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class App {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java -jar App.jar <query input directory> <output directory>");
            return;
        }

        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tInput dir: " + arg1);
        String arg2 = args[1];
        System.out.println("\tOutput dir: " + arg2);

        // Queries we skip in test set, for whatever reason, but usally bc they cause
        // time outs and we dont have to optimize them
        Set<String> badQueries = new HashSet<>(
                Arrays.asList("q19.sql", "q9.sql", "q10.sql", "q21.sql"));

        // TODO: add path as a cmd line arg
        Optimizer optimizer = new Optimizer("../data.db");
        // Iterate over target queries
        File queryDir = new File(args[0]);
        for (File file : queryDir.listFiles()) {
            if (file.getName().endsWith(".sql")) {
                System.out.println("Optimizing query: " + file.getName());
                optimizer.optimize(file.getPath(), args[1],
                        !badQueries.contains(file.getName()));
            }
        }
    }
}
