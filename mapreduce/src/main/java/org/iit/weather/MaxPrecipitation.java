package org.iit.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop MapReduce for MaxPrecipitation
 * Task [2] - Part 1.2
 * =================
 *
 * Identifies the month and year with the highest total precipitation across the FULL dataset.
 *
 * OUTPUT FORMAT:
 * "2nd month in 2019 had the highest total precipitation of 300 hr"
 */
public class MaxPrecipitation {

    /* =======================
     * MAPPER
     * =======================
     *
     * Input: weather_preprocessed.csv
     * Emits:
     *   Key   -> year-month
     *   Value -> precipitation_hours
     */
    public static class MapperClass
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            // Skip CSV header
            if (value.toString().startsWith("district")) return;

            String[] p = value.toString().split(",");

            String year = p[1];
            String month = p[2];
            double precipitation = Double.parseDouble(p[3]);

            // Key format: YYYY-MM
            context.write(
                new Text(year + "-" + month),
                new DoubleWritable(precipitation)
            );
        }
    }

    /* =======================
     * REDUCER
     * =======================
     *
     * Sums precipitation per month
     * Tracks the global maximum
     */
    public static class ReducerClass
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private String maxYearMonth = "";
        private double maxPrecipitation = 0;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                              Context context) {

            double sum = 0;
            for (DoubleWritable v : values) {
                sum += v.get();
            }

            if (sum > maxPrecipitation) {
                maxPrecipitation = sum;
                maxYearMonth = key.toString();
            }
        }

        /**
         * cleanup() runs ONCE after all reducers finish.
         * Used here to emit the global maximum result.
         */
        @Override
        protected void cleanup(Context context)
                throws java.io.IOException, InterruptedException {

            String[] parts = maxYearMonth.split("-");
            int year = Integer.parseInt(parts[0]);
            int month = Integer.parseInt(parts[1]);

            String suffix =
                (month == 1) ? "st" :
                (month == 2) ? "nd" :
                (month == 3) ? "rd" : "th";

            String sentence =
                month + suffix + " month in " + year +
                " had the highest total precipitation";

            context.write(
                new Text(sentence),
                new DoubleWritable(maxPrecipitation)
            );
        }
    }

    /* =======================
     * DRIVER (MAIN METHOD)
     * ======================= */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: MaxPrecipitation <input> <output>"
            );
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Maximum Monthly Precipitation");

        job.setJarByClass(MaxPrecipitation.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
