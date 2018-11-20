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

public class Driver {

    public static class CategoryLoader
    {//                                          TODO this needs to output an id_num -> helpfullness score
        public static class CategoryMapper extends Mapper<Object, Text, IntWritable, FloatWritable>
        {

            //IntWritable one = new IntWritable(1);
            //private Text word = new Text();
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // Parse the input
                String strValue = value.toString();

                // skip empty strings
                if(strValue.length() == 0)
                    return;

                System.out.println(strValue);
//                strValue = strValue.substring(cur);
//
//                StringTokenizer itr = new StringTokenizer(strValue);
//
//                while (itr.hasMoreTokens()) {
//                    Text word = new Text();
//                    String temp = itr.nextToken();
//
//                    // clean up for profiles
//                    temp = temp.toLowerCase();
//                    temp = temp.replaceAll("[^a-zA-Z0-9]", "");
//
//                    if(!temp.isEmpty())
//                    {
//                        word.set(temp);
//                        context.write(new DociGram(word, docID), one);
//                    }
//                }
            }
        }

        public static class CategoryReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>
        {

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
