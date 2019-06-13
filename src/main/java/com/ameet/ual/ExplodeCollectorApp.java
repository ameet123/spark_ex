package com.ameet.ual;

import com.ameet.ual.conf.AppConstants;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.ameet.ual.conf.AppConstants.*;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.split;

/**
 * ameet.chaubal on 6/13/2019.
 */
public class ExplodeCollectorApp implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExplodeCollectorApp.class);
    private SparkSession spark;

    public ExplodeCollectorApp() {
        LOGGER.info(">>Starting ExploderCollector app");

        cleanupOutput();
        System.setProperty("hadoop.home.dir", HADOOP_HOME);
        spark = session();
        Dataset<Row> allRowDF = createAllRowDF();
        allRowDF.show();
        allRowDF.printSchema();
    }

    public static void main(String[] args) throws IOException {
        new ExplodeCollectorApp();
    }

    private static void cleanupOutput() {
        try {
            if (Files.exists(Paths.get(AppConstants.OUT))) {
                FileUtils.deleteDirectory(new File(AppConstants.OUT));
            }
            LOGGER.info(">>{} deleted", AppConstants.OUT);
        } catch (IOException e) {
            LOGGER.error("ERR: deleting output", e);
        }
    }

    private Dataset<Row> createAllRowDF() {
        Dataset<Row> allRowDF = spark.read().option("mode", "DROPMALFORMED").csv(AppConstants.INPUT)
                .toDF("aid", "aname", AppConstants.SEGMENT_COLNAME);
        LOGGER.info(">>Total rows in file:{}", allRowDF.count());
        allRowDF = allRowDF.
                withColumn(SEGMENT_ARRAY_COLNAME,
                        split(
                                regexp_replace(allRowDF.col(SEGMENT_COLNAME), ARRAY_CLEANUP_REGEX, "")
                                , ","
                        )
                ).drop(SEGMENT_COLNAME);
        return allRowDF;
    }

    private SparkSession session() {
        return SparkSession
                .builder().master("local")
                .appName("ExplodeCollectorApp")
                .getOrCreate();
    }
}
