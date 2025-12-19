package org.iit.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
 */
public class DistrictMonthlyStats {

    /* =======================
     * MAPPER
     * ======================= */
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

            // Apply last decade filter HERE
            if (year < 2015) return;

            String precipitation = p[3];
            String tempMean = p[4];

            // Key: district,year,month
            context.write(
                new Text(district + "," + year + "," + month),
                new Text(precipitation + "," + tempMean)
            );
        }
    }

    /* =======================
     * REDUCER
     * ======================= */
    public static class ReducerClass
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {

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

            String suffix =
                (month == 1) ? "st" :
                (month == 2) ? "nd" :
                (month == 3) ? "rd" : "th";

            String sentence =
                district + " had a total precipitation of " +
                totalPrecip + " hours with a mean temperature of " +
                String.format("%.2f", meanTemp) +
                " for " + month + suffix + " month in " + year;

            context.write(new Text(sentence), new Text(""));
        }
    }

    /* =======================
     * DRIVER (MAIN METHOD)
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
