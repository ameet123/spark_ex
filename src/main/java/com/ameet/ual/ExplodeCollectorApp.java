package com.ameet.ual;

import com.ameet.ual.model.Booking;
import com.ameet.ual.service.DataframeProcessor;
import com.ameet.ual.utils.AppUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.io.IOException;

import static com.ameet.ual.conf.AppConstants.*;
import static com.ameet.ual.utils.AppUtils.info;

/**
 * Main orchestrator and application class
 */
public class ExplodeCollectorApp implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExplodeCollectorApp.class);
    private SparkSession spark;
    private DataframeProcessor dfProcessor;

    /**
     * creates all row and sch datasets and orchestrates the various tasks in sequence
     */
    public ExplodeCollectorApp() {
        LOGGER.info(">>Starting ExploderCollector app");

        AppUtils.cleanupOutput();
        System.setProperty("hadoop.home.dir", HADOOP_HOME);
        spark = session();
        dfProcessor = new DataframeProcessor(spark);
        Dataset<Row> allRowDF = dfProcessor.createAllRowDF();
        info(allRowDF);

        Dataset<Row> schDF = dfProcessor.createSchDF();
        info(schDF);

        Dataset<Row> allRowExplodedDF = dfProcessor.explodeArray(allRowDF, SEGMENT_ARRAY_COLNAME);
        info(allRowExplodedDF);

        LOGGER.info(">>[4] Performing JOIN:");
        Dataset<Row> allRowSCHJoinDF = dfProcessor.leftOuterJoin(allRowExplodedDF, schDF, allRowJOINCols, schJOINCols);
        info(allRowSCHJoinDF);

        LOGGER.info(">>[5] Transform to booking");
        Dataset<Booking> bookingTransformDF = dfProcessor.transformToBooking(allRowSCHJoinDF);
        bookingTransformDF.printSchema();
        bookingTransformDF.show();
    }

    public static void main(String[] args) throws IOException {
        new ExplodeCollectorApp();
    }


    private SparkSession session() {
        return SparkSession
                .builder().master("local")
                .appName("ExplodeCollectorApp")
                .getOrCreate();
    }
}
