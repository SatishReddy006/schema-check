package uprising.hbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;



public class HBaseTables {
	HBaseAdmin hadmin = null;
	Configuration hconf = null;
	String hbSchemaPattern = null;
	String tableName = "metrics";
	List<String> hbtablenames = null;
	List<String> shorttablenames = null;
	List<String> schemanames = null;
	String hbTablePattern = null;

	
	public HBaseTables()
	{
		hbtablenames = new ArrayList<String>();
		shorttablenames = new ArrayList<String> ();
		schemanames = new ArrayList<String>();
		
	}
	
	
	public void setPattern(String pattern)
	{
		hbSchemaPattern = pattern;
	}
	
	public void setTablePattern(String pattern)
	{
		hbTablePattern = pattern;
	}
	
	public void setHBaseTablesParam(HBaseAdmin hadmin, Configuration conf){
		this.hadmin = hadmin;
		this.hconf = conf;
	}
	
	   
	public void fillTableAndSchemaNames() throws IOException{
		HTable table = new HTable(hconf,tableName.getBytes());
		PrefixFilter prefixFilter = new PrefixFilter(hbSchemaPattern.getBytes());
		Scan tblscan = new Scan();
   		tblscan.setFilter(prefixFilter);
		
		ResultScanner scanner = table.getScanner(new Scan());
	    for (Result result = scanner.next(); result != null; result = scanner.next()) {
	    	byte[] key = result.getRow();
	    	String string=new String(key);
	    	String[] hTable = string.split("\\.");
	    	if (hTable.length == 3) {
	    		schemanames.add(hTable[1]); 
			}
	    	hbtablenames.add(string);
	    }
	    
	    
	    table.close();
	} 
 
   
	public List<String> getSchemaNames(){
    	return schemanames;
    }
	
    public List<String> getTableNames(){
    	return hbtablenames;
    }   
    

    
    public List<String> getShortTableNames(){
    	return shorttablenames;
    }  
    
    
    
    public void fillTableNames() {
    	for(String string:hbtablenames)
    	{
    		if(string.startsWith(hbTablePattern.toUpperCase()))
    		{
    			String[] hTable = string.split("\\.");
    			shorttablenames.add(hTable[2]);
    		}
    	}	
    }
    
    
    public void listTableNames() {
    	System.out.println("The Hbase table names are ");
    	for (String s:hbtablenames){
    	  		System.out.println(s);
    	}
    	this.listShortTableNames();
    }
    
    
    
    public void listShortTableNames() {
    	System.out.println("The Hbase SHORT table names are ");
    	for (String s:shorttablenames){
    	  		System.out.println(s);
    	}
    }
    
    public void listSchemaNames() {
    	System.out.println("The HBase schema names are ");
    	for (String s:schemanames){
	  		System.out.println(s);
    	}
    }
    
}
