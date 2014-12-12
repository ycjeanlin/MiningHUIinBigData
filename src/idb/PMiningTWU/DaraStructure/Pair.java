package idb.PMiningTWU.DaraStructure;

public class Pair {
	private int itemId;
	private int utility;
	public void setItem(int id) {
		this.itemId = id;
	}
	
	public void setUtility(int utility) {
		this.utility = utility;
	}
	
	public int getUtility() {
		return utility;
	}
	
	public int getItemId(){
		return itemId;
	}
}
