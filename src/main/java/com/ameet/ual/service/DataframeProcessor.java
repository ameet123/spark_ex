package com.ameet.ual.service;

import com.ameet.ual.conf.AppConstants;
import com.ameet.ual.model.Booking;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ameet.ual.conf.AppConstants.*;
import static org.apache.spark.sql.functions.*;

/**
 * ameet.chaubal on 6/13/2019.
 */
public class DataframeProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataframeProcessor.class);
    private SparkSession spark;

    public DataframeProcessor(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> createAllRowDF() {
        Dataset<Row> allRowDF = spark.read().option("mode", "DROPMALFORMED").csv(AppConstants.INPUT)
                .toDF("aid", "aname", AppConstants.SEGMENT_COLNAME);
        allRowDF = allRowDF.
                withColumn(SEGMENT_ARRAY_COLNAME,
                        split(
                                regexp_replace(allRowDF.col(SEGMENT_COLNAME), ARRAY_CLEANUP_REGEX, "")
                                , ","
                        )
                ).drop(SEGMENT_COLNAME);
        return allRowDF;
    }

    public Dataset<Row> explodeArray(Dataset<Row> df, String column) {
        return df.withColumn(ALLROW_EXPL_COLNAME, explode(df.col(column))).drop(column);
    }

    public Dataset<Row> createSchDF() {
        return spark.read().option("mode", "DROPMALFORMED").csv(AppConstants.SCH_FILE)
                .toDF(SCH_PK_COLNAME, "booking_name", "booking_city");
    }

    /**
     * left outer join on 2 datasets with multiple columns as join columns
     * also drop columns which are duplicate
     *
     * @param driver
     * @param driven
     * @param driverCols
     * @param drivenCols
     * @return
     */
    public Dataset<Row> leftOuterJoin(Dataset<Row> driver, Dataset<Row> driven, List<String> driverCols,
                                      List<String> drivenCols) {
        Column joinExpression = null;
        for (int i = 0; i < drivenCols.size(); i++) {
            String l = driverCols.get(i);
            String r = drivenCols.get(i);
            Column currentExpr = driver.col(l).equalTo(driven.col(r));
            if (joinExpression == null) {
                joinExpression = currentExpr;
            } else {
                joinExpression = joinExpression.and(currentExpr);
            }
        }

        Dataset<Row> joinDF = driver.join(driven, joinExpression, "left_outer");
        for (String c : drivenCols) {
            joinDF = joinDF.drop(c);
        }
        return joinDF;
    }

    public Dataset<Booking> transformToBooking(Dataset<Row> df) {
        return df.map(
                (MapFunction<Row, Booking>) x ->
                        new Booking(x.<String>getAs("bid"), x.<String>getAs("booking_name"),
                                x.<String>getAs("booking_city")
                        ), Encoders.bean(Booking.class)
        );
    }
}
