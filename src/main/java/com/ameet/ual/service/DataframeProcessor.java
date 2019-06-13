package com.ameet.ual.service;

import com.ameet.ual.conf.AppConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return df.withColumn("bid", explode(df.col(column)));
    }

    public Dataset<Row> createSchDF() {
        return spark.read().option("mode", "DROPMALFORMED").csv(AppConstants.SCH_FILE)
                .toDF("sid", "booking_name", "booking_city");
    }
}
