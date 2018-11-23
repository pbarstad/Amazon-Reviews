//package pa2;

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

    /*
      Class to store <Unigram, DocumentID> as a key
    */
    public static class Review implements WritableComparable<Review> {
      //IntWritable docID;
      //Text unigram;
      Text category; FloatWritable overallScore; FloatWritable helpfulScore; Text review;

      public Review() {
        overallScore = new FloatWritable();
        helpfulScore = new FloatWritable();
        category = new Text();
        review = new Text();
      }

      public Review(FloatWritable overall, FloatWritable helpful, Text cat, Text rev)
      {
          overallScore = overall;
          helpfulScore = helpful;
          category = cat;
          review = rev;
      }

      public Review(Review that) {
        this.overallScore = new FloatWritable(that.overallScore.get());
        this.helpfulScore = new FloatWritable(that.helpfulScore.get());
        this.category = new Text(that.category.toString());
        this.review = new Text(that.review.toString());
      }

      public void set(FloatWritable overall, FloatWritable helpful, Text cat, Text rev) {
        this.overallScore = overall;
        this.helpfulScore = helpful;
        this.category = cat;
        this.review = rev;
      }

      public FloatWritable getOverall() {
        return this.overallScore;
      }

			public FloatWritable getHelpful() {
        return this.helpfulScore;
      }

      public Text getCategory() {
        return this.category;
      } 

			public Text getReview() {
        return this.review;
      }

      public void write(DataOutput out) throws IOException {
        overallScore.write(out);
        helpfulScore.write(out);
        category.write(out);
        review.write(out);
      }

      public void readFields(DataInput in) throws IOException {
        overallScore.readFields(in);
        helpfulScore.readFields(in);
        category.readFields(in);
        review.readFields(in);
      }

      public int compareTo(Review that) {
        if(that == null)
          return 0;

        // First compare by term
        /*int docComp = this.unigram.compareTo(that.unigram);
        if(docComp == 0)
        {
          // same term, compare by docID
          return this.docID.compareTo(that.docID);

        }*/
        // in the same document
        return 1;
      }

      /*public String toString() {
        return unigram.toString() + "\t" + docID.toString() + "\t";
      } */
    }

    public static class TermFreq
{
    public static int run_me(String input) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amazon Analysis");
        job.setJarByClass(TermFreq.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(OverallHelpReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("analysisOut"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

      //IntWritable one = new IntWritable(1);
      //private Text word = new Text();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
        // Parse the input
        String strValue = value.toString();

        // skip empty strings
        if(strValue.length() == 0)
          return;

				context.write(new Text(strValue), new Text(""));
        // remove title information
        /*IntWritable docID = new IntWritable();
        String strID = "";
        int carrotCount = 0;
        int cur = 0;
        while(carrotCount < 2)
        {
            if(cur > strValue.length() -1)
                return;
          if(strValue.charAt(cur) == '>')
          {
            carrotCount++;
            if(carrotCount == 1)
            {
              String temp = strValue.substring(cur+1);
              int stopPoint = temp.indexOf('<');
              strID = temp.substring(0,stopPoint);
              docID.set(Integer.parseInt(strID));
            }
          }
          ++cur;
        }
        strValue = strValue.substring(cur);

        StringTokenizer itr = new StringTokenizer(strValue);

        while (itr.hasMoreTokens()) {
          Text word = new Text();
          String temp = itr.nextToken();

          // clean up for profiles
          temp = temp.toLowerCase();
          temp = temp.replaceAll("[^a-zA-Z0-9]", "");

          if(!temp.isEmpty())
          {
            word.set(temp);
            context.write(new DociGram(word, docID), one);
          }
      } */
    }
    }
    public static class OverallHelpReduce extends Reducer<Text, Text, Text, Text> {
        //private SortedMap<DociGram, FloatWritable> freqs = new TreeMap<DociGram, FloatWritable>();
        //float maxTF = 0;
        //@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          //int sum = 0;
          for(Text temp : values)
          {
            context.write(key, temp);
          }

          /*IntWritable value = new IntWritable(sum);
          // put into hashmap and record largest value
          freqs.put(new DociGram(key), new FloatWritable(sum));
          if(sum > maxTF)
            maxTF = sum;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
          for(Map.Entry<DociGram,FloatWritable> cur: freqs.entrySet())
          {
                                                // get augmented TF here
              context.write(cur.getKey(), new FloatWritable(0.5f + 0.5f*(cur.getValue().get() / maxTF)));
          }
        }*/
    }
}

  public static void main(String[] args) throws Exception
  {
    // get Term Frequencies and Inverse Documnet Frequencies
    TermFreq.run_me(args[0]);
    //IDF.run_me(args[0]);
    //TFIDF.run_me();
    //SentenceSummary.run_me(args[0]);
}
}
}
