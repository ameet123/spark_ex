package com.ameet.ual.utils;

import com.ameet.ual.conf.AppConstants;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * ameet.chaubal on 6/13/2019.
 */
public class AppUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppUtils.class);

    public static void cleanupOutput() {
        try {
            if (Files.exists(Paths.get(AppConstants.OUT))) {
                FileUtils.deleteDirectory(new File(AppConstants.OUT));
            }
            LOGGER.info(">>{} deleted", AppConstants.OUT);
        } catch (IOException e) {
            LOGGER.error("ERR: deleting output", e);
        }
    }
    public static void info(Dataset<Row> df){
        df.show();
        df.printSchema();
    }
}
