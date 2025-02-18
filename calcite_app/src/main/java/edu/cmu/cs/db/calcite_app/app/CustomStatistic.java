package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.schema.Statistic;

/**
 * Custom statistic class for our in-memory tables
 * 
 * Right now only returns the row count
 */
public class CustomStatistic implements Statistic {

    private final double rowCount;

    /**
     * Constructor for the statistic
     * 
     * @param rowCount row count
     */
    public CustomStatistic(double rowCount) {
        this.rowCount = rowCount;
    }

    /**
     * Returns the row count
     * 
     * @return row count
     */
    @Override
    public Double getRowCount() {
        return rowCount;
    }

}
