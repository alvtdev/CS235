import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.WritableComparable;

public class Problem4 {

  //custom composite key class in order to store both location and conference
  //count as a value (as opposed to trying to manipulate a 2D array
  private static class CompKey implements 
      WritableComparable<CompKey> {
        String city;
        String year;
        public CompKey() {}
        public CompKey(String city, String year) {
          this.city = city;
          this.year = year;
        }

        public void readFields(DataInput in) throws IOException {
          this.city = WritableUtils.readString(in);
          this.year = WritableUtils.readString(in);
        }

        public void write(DataOutput out) throws IOException {
          WritableUtils.writeString(out, city);
          WritableUtils.writeString(out, year);
        }

        public int compareTo(CompKey ck) {
          if (ck == null) { 
            return 0;
          }
          int cnt = city.compareTo(ck.city);
          return cnt == 0 ? year.compareTo(ck.year) : cnt;
        }

        @Override
        //final string should be tab separated
        public String toString() {
          return city.toString() + "\t " + year.toString() + "\t" ;
        }
      }


  //mapper clas
  public static class TokenizerMapper
       extends Mapper<Object, Text, CompKey, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    //use to set value of each location occurrence
    private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();
    //private Text word1 = new Text();


    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", true)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseSkipFile(patternsFileName);
        }
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = (caseSensitive) ?
          value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      //split line based on tab since input file is tab-separated
      //listArray[2] will be location
      String[] listArray = line.split("\t+");

      //skip header
      if (listArray[2].equals("conference_location")) {
        return;
      } 
      //split conferences by space 
      //confArray[0] will be conference acronym
      //confArray[confArray.length-1] will be year
      String[] confArray = listArray[0].split(" ");
      CompKey word = new CompKey(listArray[2], confArray[confArray.length-1]);
      context.write(word, one);
    }
  }

  public static class IntSumReducer
       extends Reducer<CompKey, IntWritable, CompKey, IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(CompKey key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //String res = new String();
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
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "ConferencesPerCity");
    job.setJarByClass(Problem4.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(CompKey.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

