package uprising.compareschema;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import org.json.simple.JSONObject;
import com.google.gson.Gson;
 

public class ResultHBaseTable {
	HBaseAdmin hadmin = null;
	Configuration hconf = null;
	HTableDescriptor resTable = null;
	String tblName = null;
	String timeStamp = null;
	String pid = null;
	int rowIdx = 1;
	
	public ResultHBaseTable() {
		Date date = new java.util.Date();
		Timestamp t = new Timestamp(date.getTime());
		timeStamp = t.toString();
		String processName =   java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
		pid = String.valueOf(Long.parseLong(processName.split("@")[0]));
	}
	
	public void setTableName(String in) {
		tblName = in;
	}
	
	public void setResultHBaseTableParam(HBaseAdmin hadmin, Configuration conf){
		this.hadmin = hadmin;
		this.hconf = conf;
	}
	
	
	@SuppressWarnings("deprecation")
	public void createResTable() {
		try {
			HTableDescriptor hdes = null;
			hdes = this.getTableDes();
		    if (hdes == null){
		      resTable = new HTableDescriptor(tblName.getBytes());
		      resTable.addFamily(new HColumnDescriptor("CF1"));
		      hadmin.createTable(resTable);
		    }
		    else
		    {
		    	HTable table = new HTable(hconf, tblName.toString().toUpperCase());
		    	Scan tblscan = new Scan();
		   		tblscan.addFamily("CF1".getBytes());
		    	tblscan.setFilter(new FirstKeyOnlyFilter());
		    	ResultScanner scanner = table.getScanner(tblscan);
		    	Set<String> treeSet = new TreeSet<String>();
		    	for(Result result : scanner)
		    	{
		    		List<KeyValue> kvList=result.list();
		    		for(KeyValue kv: kvList)
		    		{
		    			treeSet.add(Bytes.toString(kv.getRow()));
		    		}
		    	}
		    	rowIdx = treeSet.size();
		    	++rowIdx;
		    	table.close();
		    }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private HTableDescriptor getTableDes(){
		 CharSequence cs = tblName;
	     HTableDescriptor resDec = null;
	     
		 try {
	     HTableDescriptor[] tableDescriptor = hadmin.listTables();
	     
	     if (tableDescriptor.length != 0) {
	     // printing all the table names.
	     for (int i=0; i<tableDescriptor.length;i++ ){
	    	 if (tableDescriptor[i].getNameAsString().equalsIgnoreCase(cs.toString())){
	    		 resDec = tableDescriptor[i];
	    		 break;
	    	 }
	       }
	     }
		} catch (Exception e) {
			 e.printStackTrace();
		}
	    return resDec;
	}
	    	 
	
	public void addTableRow(String s, int idx){
		try {
		     /*HTableDescriptor resDec = this.getTableDes();
		     if (resDec != null) {
		    	 if(! s.equals("{}"))
		    	 {
			     HTable hTable = new HTable(hconf, resDec.getNameAsString());*/
				 HTable hTable = new HTable(hconf, this.tblName.toUpperCase());
			     String rIdx = "row" + idx;
			     Put p = new Put(Bytes.toBytes(rIdx)); 
			     p.add(Bytes.toBytes("CF1"),Bytes.toBytes("PID"),Bytes.toBytes(this.pid));
			     p.add(Bytes.toBytes("CF1"),Bytes.toBytes("TimeStamp"),Bytes.toBytes(this.timeStamp));
			     p.add(Bytes.toBytes("CF1"),Bytes.toBytes("Discrepancies"),Bytes.toBytes(s));
			     hTable.put(p);
		    	 hTable.close();
		    	// }
		     //}
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	public void fillTableinfo(SchemaCheck sck){
        
	    
	    this.pid = sck.getPid();
	    this.timeStamp = sck.getTimeStamp();
	    Gson gson = new Gson();
	    /*addTableRow(sck.getJstringSchemaTable("HBASE"), rowIdx++);
	    addTableRow(sck.getJstringSchemaTable("HIVE"), rowIdx++);
	    addTableRow(sck.getJstringSchemaColumn("HBASE"), rowIdx++);
	    addTableRow(sck.getJstringSchemaColumn("HIVE"), rowIdx);*/
	    addTableRow(gson.toJson(sck), rowIdx);
	    
	}
}
