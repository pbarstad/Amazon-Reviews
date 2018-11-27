import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import com.google.gson.*;

public class Driver {

    public static class Review {
        int[] helpful = new int[2];
        float overall;
        String reviewText;
        //String summary;
    }

    public static class CategoryLoader {//                                          TODO this needs to output an id_num -> helpfullness score

        public static class CategoryMapper extends Mapper<Object, Text, Text, FloatWritable> {

            //IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // Parse the input
                String strValue = value.toString();

                // skip empty strings
                if (strValue.length() == 0)
                    return;

                Gson g = new Gson();
                Review r = g.fromJson(strValue, Review.class);


                if (r.reviewText.length() <= 120) {
                    context.write(new Text("120 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 150) {
                    context.write(new Text("150 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 180) {
                    context.write(new Text("180 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 240) {
                    context.write(new Text("240 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 300) {
                    context.write(new Text("300 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 380) {
                    context.write(new Text("380 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 500) {
                    context.write(new Text("500 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 700) {
                    context.write(new Text("700 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 950) {
                    context.write(new Text("950 characters or less"), new FloatWritable(r.overall));
                } else if (r.reviewText.length() <= 1300) {
                    context.write(new Text("1300 characters or less"), new FloatWritable(r.overall));
                }  else {
                    context.write(new Text("1300 characters or more"), new FloatWritable(r.overall));
                }

            }
        }

        public static class CategoryReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
            //private SortedMap<DociGram, FloatWritable> freqs = new TreeMap<DociGram, FloatWritable>();
            //float maxTF = 0;
            //@Override
            public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
                float sum = 0;
                float total = 0;
                for (FloatWritable temp : values) {
                    sum += temp.get();
                    total++;
                    //context.write(key, temp);
                }

                context.write(key, new FloatWritable(sum / total));
                context.write(key, new FloatWritable(total));

            }
        }


        public static int run_me(String input) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "CategoryLoader");
            job.setJarByClass(CategoryLoader.class);
            job.setMapperClass(CategoryMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(CategoryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path("amazonOutput"));
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        // get Term Frequencies and Inverse Documnet Frequencies
        CategoryLoader.run_me(args[0]);
    }
}
