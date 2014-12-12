package idb.PHUI.DaraStructure;

import java.util.ArrayList;

public class Itemset {

	ArrayList<Integer> itemset = new ArrayList<Integer>();
	long utility;
	
	public Itemset(){
		
	}

	public void addItem(int key) {
		itemset.add(key);
	}

	public void setUtility(long value) {
		utility = value;
	}

	public long getUtility() {
		return utility;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		
		for(int item:itemset){
			sb.append(item+" ");
		}
		
		return sb.toString();
	}
}
