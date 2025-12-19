package org.iit.weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

/**
 * Hadoop MapReduce for MaxPrecipitation
 * Task [2] - Part 1.2
 * ----------------
 *
 * Calculates:
 *  - Total precipitation hours per year-month
 *
 * The maximum value from the output
 * identifies the month and year with the highest precipitation.
 */
public class MaxPrecipitation {

    /**
     * Mapper
     * ------
     * Groups precipitation by year-month.
     *
     * Key   -> year-month
     * Value -> precipitation_hours
     */
    public static class MapperClass
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (value.toString().startsWith("district")) return;

            String[] p = value.toString().split(",");

            String year = p[1];
            String month = p[2];
            double precip = Double.parseDouble(p[3]);

            context.write(
                new Text(year + "-" + month),
                new DoubleWritable(precip)
            );
        }
    }

    /**
     * Reducer
     * -------
     * Sums precipitation hours for each year-month.
     */
    public static class ReducerClass
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (DoubleWritable v : values) {
                sum += v.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }
}
