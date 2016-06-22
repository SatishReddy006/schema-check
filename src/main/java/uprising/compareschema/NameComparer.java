package uprising.compareschema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class NameComparer {
	
	public List<String> matchlist = null;
	public List<String> unmatchlist_hb = null;
	public List<String> unmatchlist_hv = null;
	
	public NameComparer() {
		matchlist = new ArrayList<String> ();
		unmatchlist_hb = new ArrayList<String> ();
		unmatchlist_hv = new ArrayList<String> ();
	}

	
    public void compareNames(List<String> btbl, List<String> ctbl, String base){
    	List<String> hbtbl = btbl;
    	List<String> hctbl = ctbl;
    	String hb = null;
    	String hc = null;
    	boolean match = false;
    	
    	for (int i = 0; i < hbtbl.size(); i ++) {
    		match = false;
    		for (int j = 0; j < hctbl.size(); j++){
    			hb = hbtbl.get(i).toLowerCase();
    			hc = hctbl.get(j).toLowerCase();
    			if (hb.equalsIgnoreCase(hc) == true) {
    				match = true;
    				matchlist.add(hb);
    				break;
    			}
    		}
    		if (!match) {
    			if (base.equalsIgnoreCase("HBASE")) unmatchlist_hb.add(hb);
    			if (base.equalsIgnoreCase("HIVE")) unmatchlist_hv.add(hb);
    		}
    		
    	}
    	this.removedups();
    }
    

    
    public void removedups()
    {
   	 Set<String> hs = new HashSet<String>();
   	 
   	 for (String s:matchlist) {
   		 hs.add(s.toLowerCase());
   	 }
   	 
   	 matchlist.clear();
   	 matchlist.addAll(hs);
    }
 
    
    public List<String> getUnmatchedList(String base)
    {
    	List<String> ret = null;
    	
    	if (base.equalsIgnoreCase("HBASE")) ret = this.unmatchlist_hb;
    	if (base.equalsIgnoreCase("HIVE")) ret = this.unmatchlist_hv;
    	
    	Set<String> unMatchedList = new HashSet<String>();
    	unMatchedList.addAll(ret);
   	 	ret.clear();
   	 	ret.addAll(unMatchedList); 
   	 	
    	return ret;
    }
    
    
    public List<String> getMatchedList()
    {
    	return this.matchlist;
    }
    
    
    public boolean isMatched(String in, String base) {
    	boolean result = true;
    	List<String> unmatchedtbl = null;
    	
    	if (base.equalsIgnoreCase("HBASE")) unmatchedtbl = this.unmatchlist_hb;
    	if (base.equalsIgnoreCase("HIVE")) unmatchedtbl = this.unmatchlist_hv;
    	
    	for (String u: unmatchedtbl) {
    		if (in.equalsIgnoreCase(u) == true) {
    			result = false;
    			return result;
    		}
    	}
    	return result;
    }

	
	public void listMatchItems(){
		System.out.println("The matched items are ");
		for (String s:matchlist){
			System.out.println(s);
		}
	}

	
	public void listUnMatchItems(String base){
		System.out.println("The unmatched items of " + base + " are ");
		List<String> unmatchedtbl = null;
    	if (base.equalsIgnoreCase("HBASE")) unmatchedtbl = this.unmatchlist_hb;
    	if (base.equalsIgnoreCase("HIVE")) unmatchedtbl = this.unmatchlist_hv;
    	
		for (String s:unmatchedtbl){
			System.out.println(s);
		}
	}

}