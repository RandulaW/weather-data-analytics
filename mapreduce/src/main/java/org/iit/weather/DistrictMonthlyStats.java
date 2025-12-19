package org.iit.weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

/**
 * Hadoop MapReduce for DistrictMonthlyStats
 * Task [2] - Part 1.1
 * --------------------
 *
 * Calculates:
 *  - Total precipitation hours
 *  - Mean temperature
 *
 * GROUPED BY:
 *  district + year + month
 */
public class DistrictMonthlyStats {

    /**
     * Mapper
     * ------
     * Input: One line from preprocessed CSV
     * Output:
     *   Key   -> district-year-month
     *   Value -> precipitation_hours, temperature_mean
     */
    public static class MapperClass
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Skip header line
            if (value.toString().startsWith("district")) return;

            String[] p = value.toString().split(",");

            String district = p[0];
            String year = p[1];
            String month = p[2];
            String precip = p[3];
            String temp = p[4];

            // Composite key ensures monthly aggregation per district
            context.write(
                new Text(district + "-" + year + "-" + month),
                new Text(precip + "," + temp)
            );
        }
    }

    /**
     * Reducer
     * -------
     * Aggregates all records for the same district-year-month
     * Calculates:
     *  - SUM of precipitation hours
     *  - MEAN of temperature
     */
    public static class ReducerClass
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalPrecip = 0;
            double totalTemp = 0;
            int count = 0;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                totalPrecip += Double.parseDouble(p[0]);
                totalTemp += Double.parseDouble(p[1]);
                count++;
            }

            double meanTemp = totalTemp / count;

            context.write(
                key,
                new Text(
                    "TotalPrecipitationHours=" + totalPrecip +
                    ", MeanTemperature=" + meanTemp
                )
            );
        }
    }
}
