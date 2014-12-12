package idb.PMiningTWU;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MiningTWUReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {

	private long  minUtility;
	
	@Override
	public void reduce(Text key, Iterator<Text> patterns,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		HashMap<String, Long> L1 = new HashMap<String, Long>();
		ArrayList<String> rmItems = new ArrayList<String>();
		ArrayList<String> frItems = new ArrayList<String>();
		List<List<String>> projDB = new ArrayList<List<String>>();
		Long keyUtility = 0L;

		while(patterns.hasNext()){
			String strPattern = patterns.next().toString();
			String[] partition = strPattern.split(":");
			String[] items = partition[0].split(" ");
			List<String> transaction = new ArrayList<String>();
			transaction.add(partition[1]);
			Long utility = Long.parseLong(partition[1]);
			keyUtility += utility;
			for(String item:items){
				transaction.add(item);
				if(L1.containsKey(item)){
					Long sumUtility = L1.get(item);
					sumUtility += utility;
					L1.put(item, sumUtility);
				}else{
					L1.put(item, utility);
				}
			}
			projDB.add(transaction);
		}
		
		Iterator<Entry<String, Long>> it = L1.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,Long> pairs = (Map.Entry<String,Long>)it.next();
	        if(((Long) pairs.getValue()) >= minUtility){
				frItems.add((String)pairs.getKey());
			}
	    }
	    
	    if(keyUtility >= minUtility){
	    	Text TWUItemset = new Text();
	    	TWUItemset.set(key.toString());
	    	output.collect(TWUItemset, new Text("-:"+keyUtility));
	    }
	    
	    for(List<String> transaction:projDB){
	    	Text PTWUItemset = new Text();

	    	StringBuilder newItemset = new StringBuilder();
	    	for(int i=1;i<transaction.size();i++){
	    		if(frItems.contains(transaction.get(i))){
	    			newItemset.append(transaction.get(i)+" ");
	    		}
	    	}
	    	
	    	if(newItemset.toString().trim().length() != 0){
	    		PTWUItemset.set("-"+newItemset.toString()+":"+transaction.get(0));
		    	output.collect(key, PTWUItemset);
	    	}
	    	
	    }
      
	}
	
	
	public void configure(JobConf job) {
	    minUtility = job.getInt("MIN_UTILITY",10000);
	}
}
