package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.schema.Statistic;

public class CustomStatistic implements Statistic {

    private final double rowCount;

    public CustomStatistic(double rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public Double getRowCount() {
        return rowCount;
    }
}
