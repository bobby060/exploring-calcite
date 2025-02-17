package edu.cmu.cs.db.calcite_app.app;

import java.util.HashSet;

public class SubQueryValidator {

    public static String validate(String sql) {
        int startIndex = 0;
        int endIndex = 0;

        int openParenCount = 0;
        int closeParenCount = 0;

        HashSet<String> namedRelations = new HashSet<>();

        String fromRelation = "";

        String validatedSql = "";

        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '(' && openParenCount == 0) {
                startIndex = i;
            }
            if (sql.charAt(i) == ')' && closeParenCount == openParenCount) {
                endIndex = i;
                // recursive call here
                String subQuery = sql.substring(startIndex, endIndex + 1);
                String validatedSubQuery = validate(subQuery);
                validatedSql = sql.substring(0, startIndex) + validatedSubQuery
                        + sql.substring(endIndex + 1);
                return validatedSql;
            }
            if (sql.charAt(i) == '(') {
                openParenCount++;
            }
            if (sql.charAt(i) == ')') {
                closeParenCount++;
            }

            // Within our current sub query, check to ensure named relations are valid
            if (openParenCount == 0) {
                // Set our candidate to replace
                if (sql.substring(i, i + 4).equals("FROM")) {
                    namedRelations.add(sql.substring(i + 6, sql.indexOf("\"", i + 6)));
                    fromRelation = sql.substring(i + 6, sql.indexOf("\"", i + 6));
                }

                //
                if (sql.substring(i, i + 2).equals("AS")) {
                    String alias = sql.substring(i + 3, sql.indexOf("\"", i + 3));
                    namedRelations.add(alias);
                }

                if (sql.substring(i, i + 5).equals("WHERE")) {
                    String namedRelation = sql.substring(i + 7, sql.indexOf("\"", i + 7));
                    if (!namedRelations.contains(namedRelation)) {
                        System.out.println("Error: " + namedRelation + " is not a named relation");
                        validatedSql = sql.substring(0, i + 5) + fromRelation
                                + sql.substring(i + namedRelation.length() + 1);

                        return validatedSql;
                    }
                }
            }

        }

        return (validatedSql.length() > 0) ? validatedSql : sql;
    }

}
