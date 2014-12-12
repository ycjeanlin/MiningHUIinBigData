package idb.PHUI.PHUI_plus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PHUIMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text item = new Text();
	private Text suffix = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String[] partition1 = value.toString().split("-");//key -itemset:utility set
		String[] partition2 = partition1[1].split(":");//itemset:utility set
		String prefix = partition1[0].trim();
		ArrayList<String> transaction = new ArrayList<String>();
		ArrayList<Long> utilitySet = new ArrayList<Long>();
		
		StringTokenizer token = new StringTokenizer( partition2[0], " ");
		
		while(token.hasMoreTokens()){
			transaction.add(token.nextToken());
		}
		
		token = new StringTokenizer( partition2[2], " ");
		
		if(prefix.length() == 0){
			utilitySet.add(0L);
		}
		
		while(token.hasMoreTokens()){
			utilitySet.add(Long.parseLong(token.nextToken()));
		}
		
		long potentialUtil = 0L;
		for(int i=0;i<transaction.size();i++){
			
			item.set(prefix+" "+transaction.get(i));
			StringBuilder sbTrans = new StringBuilder();
			StringBuilder sbUtility = new StringBuilder();
			
			utilitySet.set(i+1, utilitySet.get(0)+utilitySet.get(i+1));
			sbUtility.append(utilitySet.get(i+1)+" ");
			potentialUtil = utilitySet.get(i+1);
			for(int j=i+1;j<transaction.size();j++){
				sbTrans.append(transaction.get(j)+" ");
				sbUtility.append(utilitySet.get(j+1)+" ");
				potentialUtil += utilitySet.get(j+1);
			}
			
			//itemset:potential utility:utility set
			suffix.set(sbTrans.toString().trim()+":"+potentialUtil+":"+sbUtility.toString().trim());
			output.collect(item, suffix);
		}
	}
}
