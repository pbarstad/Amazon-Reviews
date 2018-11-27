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

        public static class CategoryMapper extends Mapper<Object, Text, Text, FloatWritable>
        {
            // dummy key that increments each write so that reducer will be called correct number of times
            int reviewNum = 0;
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


                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toLowerCase();

                if(fileName.contains("electronics"))
                    context.write(new Text("electronics " + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("automotive"))
                    context.write(new Text("automotive " + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("instant_video"))
                    context.write(new Text("instant_video" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("android"))
                    context.write(new Text("android" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("baby"))
                    context.write(new Text("baby" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("beauty"))
                    context.write(new Text("beauty" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("vinyl"))
                    context.write(new Text("vinyl" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("cell_phones"))
                    context.write(new Text("cell_phones" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("clothing"))
                    context.write(new Text("clothing" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("digital_music"))
                    context.write(new Text("digital_music" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("grocery"))
                    context.write(new Text("grocery" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("health"))
                    context.write(new Text("health" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("kitchen"))
                    context.write(new Text("kitchen" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("kindle"))
                    context.write(new Text("kindle" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("movies_and_tv"))
                    context.write(new Text("movies_and_tv" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("musical_instruments"))
                    context.write(new Text("musical_instruments" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("office_products"))
                    context.write(new Text("office_products" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("patio"))
                    context.write(new Text("patio" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("pet_supplies"))
                    context.write(new Text("pet_supplies" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("sports"))
                    context.write(new Text("sports" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("tools"))
                    context.write(new Text("tools" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("toys_and_games"))
                    context.write(new Text("toys_and_games" + reviewNum), new FloatWritable(helpful_percent));
                else if(fileName.contains("video_games"))
                    context.write(new Text("video_games" + reviewNum), new FloatWritable(helpful_percent));


                ++reviewNum;
            }
        }

        public static class CategoryReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
        {
            public Map<String, Float> helpfulness = new HashMap<String, Float>();
            public Map<String, Integer> num_by_category = new HashMap<String, Integer>();
            //float average_helpfulness = 0;
            //int num_reviews = 0;

            @Override
            public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
                if(key.toString().contains("automotive"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("automotive", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("automotive", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("automotive", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("automotive", -1);
                    if(tempCount != -1)
                        num_by_category.put("automotive", tempCount + 1);
                    else
                        num_by_category.put("automotive", 1);
                }
                else if(key.toString().contains("electronics"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("electronics", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("electronics", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("electronics", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("electronics", -1);
                    if(tempCount != -1)
                        num_by_category.put("electronics", tempCount + 1);
                    else
                        num_by_category.put("electronics", 1);
                }
                else if(key.toString().contains("instant_video"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("instant_video", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("instant_video", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("instant_video", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("instant_video", -1);
                    if(tempCount != -1)
                        num_by_category.put("instant_video", tempCount + 1);
                    else
                        num_by_category.put("instant_video", 1);
                }
                else if(key.toString().contains("android"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("android", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("android", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("android", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("android", -1);
                    if(tempCount != -1)
                        num_by_category.put("android", tempCount + 1);
                    else
                        num_by_category.put("android", 1);
                }
                else if(key.toString().contains("baby"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("baby", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("baby", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("baby", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("baby", -1);
                    if(tempCount != -1)
                        num_by_category.put("baby", tempCount + 1);
                    else
                        num_by_category.put("baby", 1);
                }
                else if(key.toString().contains("beauty"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("beauty", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("beauty", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("beauty", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("beauty", -1);
                    if(tempCount != -1)
                        num_by_category.put("beauty", tempCount + 1);
                    else
                        num_by_category.put("beauty", 1);
                }
                else if(key.toString().contains("vinyl"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("vinyl", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("vinyl", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("vinyl", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("vinyl", -1);
                    if(tempCount != -1)
                        num_by_category.put("vinyl", tempCount + 1);
                    else
                        num_by_category.put("vinyl", 1);
                }
                else if(key.toString().contains("cell_phones"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("cell_phones", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("cell_phones", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("cell_phones", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("cell_phones", -1);
                    if(tempCount != -1)
                        num_by_category.put("cell_phones", tempCount + 1);
                    else
                        num_by_category.put("cell_phones", 1);
                }
                else if(key.toString().contains("clothing"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("clothing", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("clothing", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("clothing", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("clothing", -1);
                    if(tempCount != -1)
                        num_by_category.put("clothing", tempCount + 1);
                    else
                        num_by_category.put("clothing", 1);
                }
                else if(key.toString().contains("digital_music"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("digital_music", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("digital_music", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("digital_music", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("digital_music", -1);
                    if(tempCount != -1)
                        num_by_category.put("digital_music", tempCount + 1);
                    else
                        num_by_category.put("digital_music", 1);
                }
                else if(key.toString().contains("grocery"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("grocery", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("grocery", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("grocery", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("grocery", -1);
                    if(tempCount != -1)
                        num_by_category.put("grocery", tempCount + 1);
                    else
                        num_by_category.put("grocery", 1);
                }
                else if(key.toString().contains("health"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("health", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("health", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("health", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("health", -1);
                    if(tempCount != -1)
                        num_by_category.put("health", tempCount + 1);
                    else
                        num_by_category.put("health", 1);
                }
                else if(key.toString().contains("kitchen"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("kitchen", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("kitchen", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("kitchen", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("kitchen", -1);
                    if(tempCount != -1)
                        num_by_category.put("kitchen", tempCount + 1);
                    else
                        num_by_category.put("kitchen", 1);
                }
                else if(key.toString().contains("kindle"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("kindle", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("kindle", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("kindle", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("kindle", -1);
                    if(tempCount != -1)
                        num_by_category.put("kindle", tempCount + 1);
                    else
                        num_by_category.put("kindle", 1);
                }
                else if(key.toString().contains("movies_and_tv"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("movies_and_tv", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("movies_and_tv", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("movies_and_tv", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("movies_and_tv", -1);
                    if(tempCount != -1)
                        num_by_category.put("movies_and_tv", tempCount + 1);
                    else
                        num_by_category.put("movies_and_tv", 1);
                }
                else if(key.toString().contains("musical_instruments"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("musical_instruments", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("musical_instruments", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("musical_instruments", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("musical_instruments", -1);
                    if(tempCount != -1)
                        num_by_category.put("musical_instruments", tempCount + 1);
                    else
                        num_by_category.put("musical_instruments", 1);
                }
                else if(key.toString().contains("office_products"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("office_products", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("office_products", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("office_products", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("office_products", -1);
                    if(tempCount != -1)
                        num_by_category.put("office_products", tempCount + 1);
                    else
                        num_by_category.put("office_products", 1);
                }
                else if(key.toString().contains("patio"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("patio", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("patio", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("patio", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("patio", -1);
                    if(tempCount != -1)
                        num_by_category.put("patio", tempCount + 1);
                    else
                        num_by_category.put("patio", 1);
                }
                else if(key.toString().contains("pet_supplies"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("pet_supplies", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("pet_supplies", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("pet_supplies", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("pet_supplies", -1);
                    if(tempCount != -1)
                        num_by_category.put("pet_supplies", tempCount + 1);
                    else
                        num_by_category.put("pet_supplies", 1);
                }
                else if(key.toString().contains("sports"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("sports", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("sports", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("sports", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("sports", -1);
                    if(tempCount != -1)
                        num_by_category.put("sports", tempCount + 1);
                    else
                        num_by_category.put("sports", 1);
                }
                else if(key.toString().contains("tools"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("tools", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("tools", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("tools", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("tools", -1);
                    if(tempCount != -1)
                        num_by_category.put("tools", tempCount + 1);
                    else
                        num_by_category.put("tools", 1);
                }
                else if(key.toString().contains("toys_and_games"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("toys_and_games", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("toys_and_games", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("toys_and_games", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("toys_and_games", -1);
                    if(tempCount != -1)
                        num_by_category.put("toys_and_games", tempCount + 1);
                    else
                        num_by_category.put("toys_and_games", 1);
                }
                else if(key.toString().contains("video_games"))
                {
                    float tempHelp = (float)helpfulness.getOrDefault("video_games", -1.0f);
                    if(tempHelp != -1.0f)
                        helpfulness.put("video_games", tempHelp + values.iterator().next().get());
                    else
                        helpfulness.put("video_games", values.iterator().next().get());

                    int tempCount = (int)num_by_category.getOrDefault("video_games", -1);
                    if(tempCount != -1)
                        num_by_category.put("video_games", tempCount + 1);
                    else
                        num_by_category.put("video_games", 1);
                }
            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException{
                // write the total average
               // context.write(new Text("TODO HERE!!!"), new FloatWritable(average_helpfulness / num_reviews));
                float totalScore = (float)helpfulness.get("automotive");
                int numReviews = (int)num_by_category.get("automotive");
                context.write(new Text("automotive"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("electronics");
                numReviews = (int)num_by_category.get("electronics");
                context.write(new Text("electronics"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("instant_video");
                numReviews = (int)num_by_category.get("instant_video");
                context.write(new Text("instant_video"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("android");
                numReviews = (int)num_by_category.get("android");
                context.write(new Text("android"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("baby");
                numReviews = (int)num_by_category.get("baby");
                context.write(new Text("baby"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("beauty");
                numReviews = (int)num_by_category.get("beauty");
                context.write(new Text("beauty"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("vinyl");
                numReviews = (int)num_by_category.get("vinyl");
                context.write(new Text("vinyl"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("cell_phones");
                numReviews = (int)num_by_category.get("cell_phones");
                context.write(new Text("cell_phones"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("clothing");
                numReviews = (int)num_by_category.get("clothing");
                context.write(new Text("clothing"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("digital_music");
                numReviews = (int)num_by_category.get("digital_music");
                context.write(new Text("digital_music"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("grocery");
                numReviews = (int)num_by_category.get("grocery");
                context.write(new Text("grocery"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("health");
                numReviews = (int)num_by_category.get("health");
                context.write(new Text("health"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("kitchen");
                numReviews = (int)num_by_category.get("kitchen");
                context.write(new Text("kitchen"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("kindle");
                numReviews = (int)num_by_category.get("kindle");
                context.write(new Text("kindle"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("movies_and_tv");
                numReviews = (int)num_by_category.get("movies_and_tv");
                context.write(new Text("movies_and_tv"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("musical_instruments");
                numReviews = (int)num_by_category.get("musical_instruments");
                context.write(new Text("musical_instruments"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("office_products");
                numReviews = (int)num_by_category.get("office_products");
                context.write(new Text("office_products"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("patio");
                numReviews = (int)num_by_category.get("patio");
                context.write(new Text("patio"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("pet_supplies");
                numReviews = (int)num_by_category.get("pet_supplies");
                context.write(new Text("pet_supplies"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("sports");
                numReviews = (int)num_by_category.get("sports");
                context.write(new Text("sports"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("tools");
                numReviews = (int)num_by_category.get("tools");
                context.write(new Text("tools"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("toys_and_games");
                numReviews = (int)num_by_category.get("toys_and_games");
                context.write(new Text("toys_and_games"), new FloatWritable(totalScore / numReviews));

                totalScore = (float)helpfulness.get("video_games");
                numReviews = (int)num_by_category.get("video_games");
                context.write(new Text("video_games"), new FloatWritable(totalScore / numReviews));
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
            FileOutputFormat.setOutputPath(job, new Path("category_loader"));
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        CategoryLoader.run_me(args[0]);
    }
}
