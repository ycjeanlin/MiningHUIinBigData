package idb.PHUI;


import idb.PHUI.DaraStructure.Pair;

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


public class PHUIReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {

	private int  minUtility;
	
	@Override
	public void reduce(Text key, Iterator<Text> patterns,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		HashMap<Integer, Long> L1 = new HashMap<Integer, Long>();
		ArrayList<Integer> frItems = new ArrayList<Integer>();
		List<List<Pair>> projDB = new ArrayList<List<Pair>>();
		long keyUtility = 0L;
		
		while(patterns.hasNext()){
			String strPattern = patterns.next().toString();
			List<Pair> transaction = new ArrayList<Pair>();
			keyUtility += patternParser(transaction, strPattern,L1);
			projDB.add(transaction);
			
		}
		
		Iterator<Entry<Integer, Long>> it = L1.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<Integer,Long> pairs = (Map.Entry<Integer,Long>)it.next();
	        if(((Long) pairs.getValue()) >= minUtility){
				frItems.add((Integer)pairs.getKey());
			}
	    }
	    
	    if(keyUtility >= minUtility){
	    	output.collect(key, new Text("-:"+keyUtility));
	    }
    	
	    for(List<Pair> transaction:projDB){
	    	Text PTWUItemset = new Text();

	    	StringBuilder newItemset = new StringBuilder();
	    	StringBuilder newUtilitySet = new StringBuilder();
	    	newUtilitySet.append(transaction.get(0).getUtility()+" ");//for storing key utility
	    	for(int i=1;i<transaction.size();i++){
	    		if(frItems.contains(transaction.get(i).getItemId())){
	    			newItemset.append(transaction.get(i).getItemId()+" ");
	    			newUtilitySet.append(transaction.get(i).getUtility()+" ");
	    		}
	    	}
	    	
	    	if(newItemset.toString().length() != 0){
	    		PTWUItemset.set("-"+newItemset.toString().trim()+":0:"+newUtilitySet.toString().trim());
		    	output.collect(key, PTWUItemset);
	    	}
	    	
	    }
      
	}
	
	
	private long patternParser(List<Pair> transaction, String strPattern, HashMap<Integer, Long> L1) {
		long potentialUtility = 0L;
		long keyUtility = 0L;
		String[] partition = strPattern.split(":");
		String[] items = partition[0].split(" ");
		potentialUtility = Long.parseLong(partition[1]);
		String[] utilitySet = partition[2].split(" ");
		
		keyUtility = Long.parseLong(utilitySet[0]);
		Pair newItem = new Pair();
		newItem.setItem(0);//mark as key
		newItem.setUtility(Integer.parseInt(utilitySet[0]));
		transaction.add(newItem);
		
		for(int i=0;i<items.length;i++){
			if(items[i].length() == 0){
				continue;
			}
			
			//add new item to projected database
			newItem = new Pair();
			newItem.setItem(Integer.parseInt(items[i]));
			newItem.setUtility(Integer.parseInt(utilitySet[i+1]));
			transaction.add(newItem);
			
			//potential utility counting
			if(L1.containsKey(newItem.getItemId())){
				long sumUtility = L1.get(newItem.getItemId());
				sumUtility += potentialUtility;
				L1.put(newItem.getItemId(), sumUtility);
			}else{
				L1.put(newItem.getItemId(), potentialUtility);
			}
		}
		return keyUtility;
	}


	public void configure(JobConf job) {
	    minUtility = job.getInt("MIN_UTILITY",10000);
	}
}
