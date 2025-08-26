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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

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

    public static class TopKMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outCount = new IntWritable();
        private final Text outCoord = new Text();

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            int tab = line.lastIndexOf('\t');
            if (tab < 0) return;

            String coord = line.substring(0, tab);
            String cntStr = line.substring(tab + 1);
            int cnt;
            try { cnt = Integer.parseInt(cntStr); }
            catch (NumberFormatException e) { return; }

            outCount.set(cnt);     // key = count
            outCoord.set(coord);   // val = "lat,lon"
            ctx.write(outCount, outCoord);
        }
    }

    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() { super(IntWritable.class, true); }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable x = (IntWritable) a;
            IntWritable y = (IntWritable) b;
            return Integer.compare(y.get(), x.get()); // reverse
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // reuse IntWritable byte comparator but reverse sign
            return -IntWritable.Comparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class TopKReducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
        private int K;
        private int emitted = 0;
        private final IntWritable outCount = new IntWritable();

        @Override
        protected void setup(Context ctx) {
            K = ctx.getConfiguration().getInt("topk", 10);
        }

        @Override
        public void reduce(IntWritable count, Iterable<Text> coords, Context ctx)
                throws IOException, InterruptedException {

            for (Text coord : coords) {
                if (emitted >= K) return;
                outCount.set(count.get());
                ctx.write(coord, outCount);
                emitted++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int K = 10;

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "count of drop off locations");
        job1.setJarByClass(Lab3.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) System.exit(1);

        Configuration conf2 = new Configuration();
        conf2.setInt("topk", K);
        Job job2 = Job.getInstance(conf2, "top-K drop off locations");
        job2.setJarByClass(Lab3.class);
        job2.setMapperClass(TopKMapper2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(TopKReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(DescendingIntComparator.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

// http://localhost:8088
