package idb.PMiningTWU;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MiningTWUMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text item = new Text();
	private Text suffix = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String[] partition1 = value.toString().split("-");
		String[] partition2 = partition1[1].split(":");
		String prefix = partition1[0].trim();
		LongWritable utility = new LongWritable(Long.parseLong(partition2[1]));
		
		ArrayList<String> transaction = new ArrayList<String>();
		StringTokenizer token = new StringTokenizer( partition2[0], " ");
		
		while(token.hasMoreTokens()){
			transaction.add(token.nextToken());
		}
		
		
		for(int i=0;i<transaction.size();i++){
			item.set(prefix+" "+transaction.get(i));
			
			StringBuilder sb = new StringBuilder();
			for(int j=i+1;j<transaction.size();j++){
				sb.append(transaction.get(j)+" ");
			}
			
			suffix.set(sb.toString().trim()+":"+utility);
			output.collect(item, suffix);
		}
	}
}
