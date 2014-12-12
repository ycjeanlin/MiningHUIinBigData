/*
 * Compile: javac -classpath hadoop-core-0.20.205.0.jar -d PMTWU_classes UtilityCount.java 
 * Create runable jar file: jar -cvf ./UtilityCount.jar -C PMTWU_classes/ .
 * Execution: hadoop jar UtilityCount.jar idb.PMining.UtilityCount  /user/user09/input /user/user09/output
 */

package idb.PMiningTWU;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class UtilityCount{

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
    private Text item = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
      String[] part = value.toString().split(":");
      String transaction = part[1];
      LongWritable utility = new LongWritable(Long.parseLong(part[0]));
      StringTokenizer tokenizer = new StringTokenizer(transaction);
      while (tokenizer.hasMoreTokens()) {
        item.set(tokenizer.nextToken());
        output.collect(item, utility);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
      long sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new LongWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(UtilityCount.class);
    conf.setJobName("UtilityCount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
