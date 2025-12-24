package org.iit.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hadoop MapReduce for DistrictMonthlyStats
 * Task [2] - Part 1.1
 * ====================
 *
 * Calculates:
 *  - Total precipitation
 *  - Mean temperature (temperature_2m_mean)
 *
 * For:
 *  - Each district
 *  - Each month
 *  - Over the last decade (2015 onwards)
 *
 * Output:
 *  - Sorted by district, year, month (default Hadoop sort)
 *  - Raw dataset
 *  - Human-readable sentence output
 */
public class DistrictMonthlyStats {

    /* =======================
     * MAPPER
     * =======================
     *
     * Input:
     *   weather_preprocessed.csv
     *
     * Emits:
     *   Key   -> district,year,month
     *   Value -> precipitation_hours,temperature_2m_mean
     *
     * Notes:
     *   - Filters records from 2015 onwards
     *   - Key structure guarantees sorted output
     */
    public static class MapperClass
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            // Skip CSV header
            if (value.toString().startsWith("district")) return;

            String[] p = value.toString().split(",");

            String district = p[0];
            int year = Integer.parseInt(p[1]);
            int month = Integer.parseInt(p[2]);

            // Filter: last decade only
            if (year < 2015) return;

            String precipitation = p[3];
            String tempMean = p[4];

            context.write(
                new Text(district + "," + year + "," + month),
                new Text(precipitation + "," + tempMean)
            );
        }
    }

    /* =======================
     * REDUCER
     * =======================
     *
     * Aggregates:
     *   - Total precipitation per district-month
     *   - Mean temperature per district-month
     *
     * Writes:
     *   1. Raw structured dataset (CSV) → for Power BI
     *   2. Sentence-based output → for reporting
     */

    public static class ReducerClass
            extends Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        private boolean headerWritten = false;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {

            // ---------- WRITE HEADER ONCE ----------
            if (!headerWritten) {
                multipleOutputs.write(
                    "raw",
                    NullWritable.get(),
                    new Text("district,year,month,total_precipitation,mean_temperature")
                );
                headerWritten = true;
            }

            double totalPrecip = 0;
            double tempSum = 0;
            int count = 0;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                totalPrecip += Double.parseDouble(p[0]);
                tempSum += Double.parseDouble(p[1]);
                count++;
            }

            double meanTemp = tempSum / count;

            String[] k = key.toString().split(",");
            String district = k[0];
            String year = k[1];
            int month = Integer.parseInt(k[2]);

            // ---------- RAW CSV OUTPUT ----------
            String rawRecord =
                district + "," +
                year + "," +
                month + "," +
                String.format("%.2f", totalPrecip) + "," +
                String.format("%.2f", meanTemp);

            multipleOutputs.write(
                "raw",
                NullWritable.get(),
                new Text(rawRecord)
            );

            // ---------- SENTENCE OUTPUT ----------
            String suffix =
                (month == 1) ? "st" :
                (month == 2) ? "nd" :
                (month == 3) ? "rd" : "th";

            String sentence =
                district + " had a total precipitation of " +
                String.format("%.2f", totalPrecip) +
                " hours with a mean temperature of " +
                String.format("%.2f", meanTemp) +
                " for the " + month + suffix +
                " month in " + year;

            multipleOutputs.write(
                "sentences",
                NullWritable.get(),
                new Text(sentence)
            );
        }

        @Override
        protected void cleanup(Context context)
                throws java.io.IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    /* =======================
     * DRIVER
     * ======================= */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: DistrictMonthlyStats <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "District Monthly Weather Statistics");

        job.setJarByClass(DistrictMonthlyStats.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Named outputs */
        MultipleOutputs.addNamedOutput(
            job,
            "raw",
            TextOutputFormat.class,
            NullWritable.class,
            Text.class
        );

        MultipleOutputs.addNamedOutput(
            job,
            "sentences",
            TextOutputFormat.class,
            NullWritable.class,
            Text.class
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
