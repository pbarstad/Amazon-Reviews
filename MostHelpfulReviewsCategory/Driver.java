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

    public static class CategoryLoader
    {
        public static class Review
        {
            int[] helpful = new int[2];
        }

        public static class CategoryMapper extends Mapper<Object, Text, IntWritable, FloatWritable>
        {
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // Parse the input
                String strValue = value.toString();

                // skip empty strings
                if(strValue.length() == 0)
                    return;

                Gson g = new Gson();
                Review r = g.fromJson(strValue, Review.class);
                float helpful_percent = (float)r.helpful[0] / (float)r.helpful[1];

                if(r.helpful[1] == 0)
                    return;

                context.write(new IntWritable(1), new FloatWritable(helpful_percent));
                System.out.println("WRITING FROM MAPPER: " + 1 + " -- " + helpful_percent);
            }
        }

        public static class CategoryReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>
        {
            float average_helpfulness = 0;
            int num_reviews = 0;

            @Override
            public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
                // calculate the running average
                ++num_reviews;
                // only one value here so just get next()
                float cur_score = values.iterator().next().get();
                average_helpfulness += cur_score;
                average_helpfulness = average_helpfulness / num_reviews;

                System.out.println("Cur average: " + average_helpfulness + " -- Num Reviews: " + num_reviews);
                //context.write(new IntWritable(num_reviews), new FloatWritable(average_helpfulness));
            }

//            @Override
//            protected void cleanup(Context context) throws IOException, InterruptedException{
//                // write the total average
//                context.write(new IntWritable(num_reviews), new FloatWritable(average_helpfulness));
//            }
        }


        public static int run_me(String input) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "CategoryLoader");
            job.setJarByClass(CategoryLoader.class);
            job.setMapperClass(CategoryMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(CategoryReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(FloatWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path("category_loader"));
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        // get Term Frequencies and Inverse Documnet Frequencies
        CategoryLoader.run_me(args[0]);
        //IDF.run_me(args[0]);
        //TFIDF.run_me();
        //SentenceSummary.run_me(args[0]);
    }
}
