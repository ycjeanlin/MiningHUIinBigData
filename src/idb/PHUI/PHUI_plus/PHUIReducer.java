package idb.PHUI.PHUI_plus;


import idb.PHUI.DaraStructure.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
		HashMap<String, List<Pair>> mergeProjDB = new HashMap<String, List<Pair>>();
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
	    
	    //merge the same transaction
	    mergeTransaction(projDB,mergeProjDB, frItems);
	    
	    //identify whather key is HUI or not
	    if(keyUtility >= minUtility){
	    	output.collect(key, new Text("-:"+keyUtility));
	    }
	    
    	if(mergeProjDB.size() > 1){
			Iterator<List<Pair>> transactions = mergeProjDB.values().iterator();
		
			 while(transactions.hasNext()){
				List<Pair> transaction = transactions.next();
			    Text PTWUItemset = new Text();
			
			    StringBuilder newItemset = new StringBuilder();
			    StringBuilder newUtilitySet = new StringBuilder();
			    newUtilitySet.append(transaction.get(0).getUtility()+" ");//for storing key utility
			    long newPotentialUtil = transaction.get(0).getUtility();
			    for(int i=1;i<transaction.size();i++){
			    	if(frItems.contains(transaction.get(i).getItemId())){
			    		newItemset.append(transaction.get(i).getItemId()+" ");
			    		newUtilitySet.append(transaction.get(i).getUtility()+" ");
			    		newPotentialUtil += transaction.get(i).getUtility();
			    	}
			    }

		    	PTWUItemset.set("-"+newItemset.toString().trim()+":"+newPotentialUtil+":"+newUtilitySet.toString().trim());
		    	output.collect(key, PTWUItemset);
			}
    	}else if(mergeProjDB.size() == 1){//single path
    		Iterator<List<Pair>> transactions = mergeProjDB.values().iterator();
    		List<Pair> transaction = transactions.next();
    		
    		for(int i=1;i<(int)Math.pow(2, transaction.size()-1);i++){
    			String bits = Integer.toBinaryString(i);
    			StringBuilder newItemset = new StringBuilder();
    			Text PHUItemset = new Text();
    			long utility = transaction.get(0).getUtility();
    			while(bits.length()<transaction.size()){
    				bits="0"+bits;
    			}
    			
    			newItemset.append(key.toString());
    			for(int j=0;j<bits.length();j++){
    				if(bits.charAt(j) == '1'){
    					utility += transaction.get(j).getUtility();
    					newItemset.append(" "+transaction.get(j).getItemId());
    				}
    			}
    			
    			if(utility >= minUtility){
					PHUItemset.set(newItemset.toString());
					output.collect(PHUItemset, new Text("-:"+utility));
				}
    		}
    	}
	    
      
	}
	
	
	private void mergeTransaction(List<List<Pair>> projDB,
			HashMap<String, List<Pair>> mergeProjDB, ArrayList<Integer> frItems) {
		for(List<Pair> transaction:projDB){
	    	StringBuilder newItemset = new StringBuilder();
	    	List<Pair> newTran = new ArrayList<Pair>();
	    	newTran.add(transaction.get(0));
	    	for(int i=1;i<transaction.size();i++){
	    		if(frItems.contains(transaction.get(i).getItemId())){
	    			newItemset.append(transaction.get(i).getItemId()+" ");
	    			newTran.add(transaction.get(i));
	    		}
	    	}
	    	
	    	if(newItemset.toString().length() != 0){
	    		String strItemset = newItemset.toString();
		    	if(mergeProjDB.containsKey(strItemset)){
		    		List<Pair> value = mergeProjDB.get(strItemset);
		    		
		    		for(int i=0;i<value.size();i++){
		    			value.get(i).setUtility(value.get(i).getUtility()+newTran.get(i).getUtility());
		    		}
		    		
		    		mergeProjDB.put(strItemset, value);
		    	}else{
		    		mergeProjDB.put(strItemset, newTran);
		    	}
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
		transaction.add(newItem);//first item in the transaction is 0(key exutility)
		
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
