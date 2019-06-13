package com.ameet.ual.conf;

import java.util.ArrayList;
import java.util.List;

/**
 * ameet.chaubal on 6/13/2019.
 */
public class AppConstants {
    public static String INPUT = "src/main/resources/pnr.csv";
    public static String SCH_FILE = "src/main/resources/sch.csv";
    public static String OUT_TOP = "out";
    public static String OUT = OUT_TOP + "/wc";
    public static String SEGMENT_COLNAME = "segments";
    public static String SEGMENT_ARRAY_COLNAME = "segments_items";
    public static String ARRAY_CLEANUP_REGEX = "[\\[\\]]";
    public static String HADOOP_HOME = "C:\\Users\\ameet.chaubal\\Documents\\software\\hadoop";
    public static String ALLROW_EXPL_COLNAME="bid";
    public static String SCH_PK_COLNAME="sid";
    public static List<String> allRowJOINCols = new ArrayList<String>() {
        {
            add(ALLROW_EXPL_COLNAME);
        }
    };
    public static List<String> schJOINCols = new ArrayList<String>() {
        {
            add(SCH_PK_COLNAME);
        }
    };

}
