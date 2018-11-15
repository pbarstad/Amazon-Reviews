package pa2;

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
    public static class DociGram implements WritableComparable<DociGram> {
      IntWritable docID;
      Text unigram;

      public DociGram() {
        docID = new IntWritable();
        unigram = new Text();
      }

      public DociGram(Text term, IntWritable ID)
      {
          unigram = term;
          docID = ID;
      }

      public DociGram(DociGram that) {
        this.docID = new IntWritable(that.docID.get());
        this.unigram = new Text(that.unigram.toString());
      }

      public void set(Text unigram, IntWritable docID) {
        this.docID = docID;
        this.unigram = unigram;
      }

      public IntWritable getDocID() {
        return this.docID;
      }

      public Text getUnigram() {
        return this.unigram;
      }

      public void write(DataOutput out) throws IOException {
        unigram.write(out);
        docID.write(out);
      }

      public void readFields(DataInput in) throws IOException {
        unigram.readFields(in);
        docID.readFields(in);
      }

      public int compareTo(DociGram that) {
        if(that == null)
          return 0;

        // First compare by term
        int docComp = this.unigram.compareTo(that.unigram);
        if(docComp == 0)
        {
          // same term, compare by docID
          return this.docID.compareTo(that.docID);

        }
        // in the same document
        return docComp;
      }

      public String toString() {
        return unigram.toString() + "\t" + docID.toString() + "\t";
      }
    }

    public static class TermFreq
{
    public static int run_me(String input) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TermFreq");
        job.setJarByClass(TermFreq.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(DociGram.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("term_freq"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, DociGram, IntWritable>{

      IntWritable one = new IntWritable(1);
      //private Text word = new Text();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the input
        String strValue = value.toString();

        // skip empty strings
        if(strValue.length() == 0)
          return;

        // remove title information
        IntWritable docID = new IntWritable();
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
      }
    }
    }
    public static class IntSumReducer extends Reducer<DociGram,IntWritable,DociGram, FloatWritable> {
        private SortedMap<DociGram, FloatWritable> freqs = new TreeMap<DociGram, FloatWritable>();
        float maxTF = 0;
        @Override
        public void reduce(DociGram key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int sum = 0;
          for(IntWritable temp : values)
          {
            sum++;
          }

          IntWritable value = new IntWritable(sum);
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
        }
    }
}

    public static class IDF {

        private static int totalArticles = 0;

        public static int run_me(String input) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "IDF");
            job.setJarByClass(IDF.class);
            job.setMapperClass(TokenizerMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(DociGram.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path("idf"));
            return job.waitForCompletion(true) ? 0 : 1;
        }

        public static class TokenizerMapper extends Mapper<Object, Text, DociGram, IntWritable>{

          IntWritable one = new IntWritable(1);
          //private Text word = new Text();
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
              ++totalArticles;
            // Parse the input
            String strValue = value.toString();

            // skip empty strings
            if(strValue.length() == 0)
              return;

            // remove title information
            IntWritable docID = new IntWritable();
            String strID = "";
            int carrotCount = 0;
            int cur = 0;
            while(carrotCount < 2)
            {
                if(cur > strValue.length()-1)
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
          }
        }
        }

        // we will be recieving keys of <term, articleID> so we know term is only
        // counted once per article
        public static class IntSumReducer extends Reducer<DociGram,IntWritable,Text,FloatWritable> {
            // store each term and how many articles it appears in
            // increment for each article we see it in
            private SortedMap<Text, FloatWritable> terms = new TreeMap<Text, FloatWritable>();

            @Override
            public void reduce(DociGram key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

                // see if we've already ran into this term
                if(terms.get(key.getUnigram()) == null)
                {
                    // haven't come across this term yet
                    Text newOne = new Text(key.getUnigram());
                    terms.put(newOne, new FloatWritable(1));
                }
                // we have already seen this term, so increment num articles it's been seen in
                else
                {
                    Text newOne = key.getUnigram();
                    float curCount = terms.get(key.getUnigram()).get();
                    curCount = curCount + 1.0f;
                    terms.put(newOne, new FloatWritable(curCount));
                }
            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException{
              for(Map.Entry<Text,FloatWritable> cur: terms.entrySet())
              {
                  // LOGbase10(N / ni)
                  float idf = (float)Math.log10((float)totalArticles / cur.getValue().get());

                  context.write(cur.getKey(), new FloatWritable(idf));
              }
            }
        }
    }

    public static class TFIDF
    {
        public static int run_me() throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TFIDF");
            job.setJarByClass(TFIDF.class);
            job.setMapperClass(TokenizerMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(DociGram.class);
            job.setOutputValueClass(FloatWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path("term_freq/part-r-00000"));
            FileInputFormat.addInputPath(job, new Path("idf/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("tfidf"));
            return job.waitForCompletion(true) ? 0 : 1;
        }

        private static SortedMap<Text, FloatWritable> idfIn = new TreeMap<Text, FloatWritable>();

        public static class TokenizerMapper extends Mapper<Object, Text, DociGram, FloatWritable>{

            // this basically just loads the TF (to reducer) and IDF (into MAP)
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input
            String strValue = value.toString();

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().toString();

            // dealing with TF
            if(filename.contains("term_freq"))
            {
                // parse and essentially re-write
                  StringTokenizer itr = new StringTokenizer(strValue);

                  Text word = new Text(itr.nextToken());
                  IntWritable docID = new IntWritable(Integer.parseInt(itr.nextToken()));
                  FloatWritable freq = new FloatWritable(Float.parseFloat(itr.nextToken()));

                  context.write(new DociGram(word, docID), freq);
            }
            // dealing with IDF
            else
            {
                // parse and put into map
                StringTokenizer itr = new StringTokenizer(strValue);

                Text word = new Text(itr.nextToken());
                FloatWritable idf = new FloatWritable(Float.parseFloat(itr.nextToken()));

                idfIn.put(word, idf);
            }
        }
        }
        public static class IntSumReducer extends Reducer<DociGram,FloatWritable, DociGram, FloatWritable> {

            // takes TF and multiplies it by IDF
            @Override
            public void reduce(DociGram key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
                    Text curTerm = key.getUnigram(); // get current term
                    float idfMultiplier = idfIn.get(curTerm).get(); // get IDF value for this term

                    float curValue = 0;
                    for(FloatWritable val : values)
                    {
                        curValue += val.get(); // there's only one anyways so it doesnt matter
                        // we are basicaly just lazy and using loop to get firsst element
                    }

                    float finalVal = curValue * idfMultiplier;
                                                      // get augmented TF here
                    context.write(new DociGram(key), new FloatWritable(finalVal));
            }
        }
    }

    public static class SentenceSummary
    {
        public static int run_me(String input) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "SentenceSummary");
            job.setJarByClass(SentenceSummary.class);
            job.setMapperClass(TokenizerMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path("tfidf/part-r-00000"));
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path("summary"));
            return job.waitForCompletion(true) ? 0 : 1;
        }
                                                                    //  docID       3 sentences
        public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{

        private static HashMap<Integer, ArrayList<MutablePair<String, FloatWritable>>> scores = new HashMap<>();

            // this basically just loads the TF (to reducer) and IDF (into MAP)
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input
            String strValue = value.toString();

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().toString();

            // load the TFIDF scores into a map for quick lookup
             if(filename.contains("tfidf"))
            {
                // parse and essentially re-write
                  StringTokenizer itr = new StringTokenizer(strValue);

                  String word = itr.nextToken();
                  Integer docID = Integer.parseInt(itr.nextToken());
                  FloatWritable score = new FloatWritable(Float.parseFloat(itr.nextToken()));

                try
                {
                    scores.get(docID).add(new MutablePair<String, FloatWritable>(word, score));
                }
                catch(NullPointerException e)
                {
                    ArrayList<MutablePair<String, FloatWritable>> temp = new ArrayList<>();
                    scores.put(docID, temp);
                }
            }
            // parse and calculate sentence scores
            else
            {
                if(strValue.length()  <= 1)
                    return;

                //get docID and move striing to actual article content
                IntWritable docID = new IntWritable();
                String strID = "";
                int carrotCount = 0;
                int cur = 0;
                while(carrotCount < 2)
                {
                    if(cur > strValue.length()-1)
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

                ArrayList<MutablePair<String, FloatWritable>> docScores = scores.get(new Integer(docID.get()));

                // our top 3 sentences
                MutablePair<Float, String> sentence1 = new MutablePair<>(0.0f, "");
                MutablePair<Float, String> sentence2 = new MutablePair<>(0.0f, "");
                MutablePair<Float, String> sentence3 = new MutablePair<>(0.0f, "");

                // extract senteences
                String[] sentences = strValue.split(Pattern.quote(". "));
                for(int i = 0; i < sentences.length; ++i)
                {
                    StringTokenizer itr = new StringTokenizer(sentences[i]);

                    ArrayList<FloatWritable> sentenceScores = new ArrayList<>();

                    // go through the sentence
                    while (itr.hasMoreTokens()) {
                        String temp = itr.nextToken();
                        if(temp.isEmpty())
                          continue;

                        // clean up for profiles
                        temp = temp.toLowerCase();
                        temp = temp.replaceAll("[^a-zA-Z0-9]", "");

                        for(int j = 0; j < docScores.size(); ++j)
                        {
                            // we found our term
                            if(docScores.get(j).getKey().equals(temp))
                            {
                                // add word scores for sentence
                                sentenceScores.add(docScores.get(j).getValue());
                            }
                        }
                  }

                  Collections.sort(sentenceScores, Collections.reverseOrder());
                  float sum = 0;
                  for(int top5 = 0; top5 < 5 && top5 < sentenceScores.size(); ++top5)
                  {
                      sum += sentenceScores.get(top5).get();
                  }

                  // set top 3 sentences
                  if(sum > sentence1.getKey() || sentence1.getValue().length() == 0)
                  {
                      sentence1 = new MutablePair<>(sum, sentences[i]);
                  }
                  else if((sum > sentence2.getKey() && sum <= sentence1.getKey()) || sentence2.getValue().length() == 0)
                  {
                      sentence2 = new MutablePair<>(sum, sentences[i]);
                  }
                  else if((sum > sentence3.getKey() && sum <= sentence2.getKey()) || sentence3.getValue().length() == 0)
                  {
                      sentence3 = new MutablePair<>(sum, sentences[i]);
                  }

                }

                String out = sentence1.getValue() + ". " + sentence2.getValue() + ". " + sentence3.getValue() + ".";
                context.write(docID, new Text(out));
            }
        }
        }
        public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

            // takes TF and multiplies it by IDF
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                    for(Text t : values)
                    {
                        context.write(key, t); // there should only be 1 value so this is cheating lazy way. not very safe but it works...
                    }
            }
        }
    }
  public static void main(String[] args) throws Exception
  {
    // get Term Frequencies and Inverse Documnet Frequencies
    TermFreq.run_me(args[0]);
    IDF.run_me(args[0]);
    TFIDF.run_me();
    SentenceSummary.run_me(args[0]);
}
}
