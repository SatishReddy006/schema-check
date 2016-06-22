package uprising.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseColumns {
	HBaseAdmin hadmin = null;
	Configuration hconf = null;
	String hbtblname = null;
	String tableName = "metrics_history";
	String cfname = null ;
	List<String> colnames = null;
	CharSequence cs = null;
	HTable table;
	Scan tblscan = null;	
	
	public HBaseColumns()
	{
		colnames = new ArrayList<String> ();
	}
	
	
	public HBaseColumns(String cfname, String tbname)
	{
		this.cfname = cfname;
		this.hbtblname = tbname;
		colnames = new ArrayList<String> ();
	}
	
	
	public void setHBaseColumnsParam(HBaseAdmin hadmin, Configuration conf){
		this.hadmin = hadmin;
		this.hconf = conf;
	}
	
	
	
	public void setPattern(String tbname,String cfname)
	{
		this.hbtblname = tbname;
		this.cfname = cfname;
	}
	
	@SuppressWarnings("deprecation")   
    public void fillColumnNames() throws IOException{
		
		HTable table = new HTable(hconf,tableName.getBytes());
		PrefixFilter prefixFilter = new PrefixFilter(hbtblname.getBytes());
		
        Scan tblscan = new Scan();
   		tblscan.addFamily((this.cfname).getBytes());
   		tblscan.setMaxResultSize(2);
   		tblscan.setFilter(prefixFilter);
   		ResultScanner scanner = table.getScanner(tblscan);
    	for(Result result : scanner)
    	{	
    		String columns=Bytes.toString(result.getValue(Bytes.toBytes(this.cfname), Bytes.toBytes("Message")));
    		if( columns != null)
    		{
    			String[] kv=columns.split("\\|\\|");
    			for(int i=0;i < kv.length;i++)
    			{
    				String[] column=kv[i].split("\\:\\:");
    				colnames.add(column[0]);
    			}
    			
    		}
    		/*List<KeyValue> kvList=result.list();
    		for(KeyValue kv: kvList)
    		{
    			String s=new String(kv.getValue());
    			String[] s1=s.split("\\|\\|");
    			for(int i=0;i < s1.length;i++)
    			{
    				String[] s2=s1[i].split("\\:\\:");
    				System.out.println(s2[0]);
    			}
    		}*/
    	}
    	
    	Set<String> col_hb = new HashSet<String>();
   	 	col_hb.addAll(colnames);
   	 	colnames.clear();
   	 	colnames.addAll(col_hb);
	    
	    table.close();
    }

	    
    public List<String> getColumnNames() {
    	return this.colnames;
    }
    
    
    public List<String> getColumnNamesofFamily(String cf){
    	return this.colnames;
    }
    
   
    public void listColumnNames() {
    	System.out.println("The Hbase column names of table " + this.hbtblname + " are ");
    	for (String column:colnames) {
    		System.out.println(column);
    	}
    }
    
}
