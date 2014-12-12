package idb.PHUI.DaraStructure;

public class Pair {
	private int itemId;
	private long utility;
	
	public void setItem(int id) {
		this.itemId = id;
	}
	
	public void setUtility(long utility) {
		this.utility = utility;
	}
	
	public long getUtility() {
		return utility;
	}
	
	public int getItemId(){
		return itemId;
	}
}
