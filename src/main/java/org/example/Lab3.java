package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lab3 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text coord = new Text();
        enum DropReason { HEADER, MISSING, BAD_PARSE, ZERO_ZERO, OUTSIDE_NYC }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            if (line.startsWith("VendorID")) {
                context.getCounter(DropReason.HEADER).increment(1);
                return; // skip the first line aka the header
            }
            String[] parts = line.split(",", -1);
            if (parts.length <= 10) {
                context.getCounter(DropReason.MISSING).increment(1);
                return;
            }

            String lonStr = parts[9].trim();
            String latStr = parts[10].trim();
            double lon, lat;

            try {
                if (lonStr.isEmpty() || latStr.isEmpty()) {
                    context.getCounter(DropReason.MISSING).increment(1);
                    return;
                }
                lon = Double.parseDouble(lonStr);
                lat = Double.parseDouble(latStr);
            } catch (NumberFormatException e) {
                context.getCounter(DropReason.BAD_PARSE).increment(1);
                return;
            }

            if (lon == 0.0 && lat == 0.0) {
                context.getCounter(DropReason.ZERO_ZERO).increment(1);
                return;
            }

            if (!(lat >= 40 && lat <= 42 && lon >= -75 && lon <= -73)) {
                context.getCounter(DropReason.OUTSIDE_NYC).increment(1);
                return;
            }

            double latBin = Math.round(lat * 100.0) / 100.0;
            double lonBin = Math.round(lon * 100.0) / 100.0;

            coord.set(latBin + "," + lonBin);
            context.write(coord, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count of drop off locations");
        job.setJarByClass(Lab3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// http://localhost:8088
