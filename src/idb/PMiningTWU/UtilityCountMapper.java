package idb.PMiningTWU;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UtilityCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
    private Text item = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
		String[] part = value.toString().split(":");
		String transaction = part[0];
		LongWritable utility = new LongWritable(Long.parseLong(part[1]));
		StringTokenizer tokenizer = new StringTokenizer(transaction, " ");
		while (tokenizer.hasMoreTokens()) {
			item.set(tokenizer.nextToken());
			output.collect(item, utility);
		}
    }
}
