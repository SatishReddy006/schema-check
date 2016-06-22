package uprising.hive;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;


public class HiveColumns {
	HiveMetaStoreClient hiveMetastoreClient = null;
	Configuration hconf = null;
	String tablename = null;
	String hivedbname = null;
	List<String> colnames = null;
	
	
	public HiveColumns(){
		colnames = new ArrayList<String> ();
	}
	
	
	public HiveColumns(String tname, String dbname){
		this.tablename = tname;
		this.hivedbname = dbname;
		colnames = new ArrayList<String> ();
	}
	
	
	
	public void setHiveColumnsParam(HiveMetaStoreClient hclient, Configuration conf){
		this.hiveMetastoreClient = hclient;
		this.hconf = conf;
	}
	
	public void setPattern(String dbname, String tname)
	{
		this.tablename = tname;
		this.hivedbname = dbname;
	}
	
	
	public void fillColumnNames() throws IOException {
		try {
    	    Database database = hiveMetastoreClient.getDatabase(this.hivedbname);
   		
    		Table table = hiveMetastoreClient.getTable(database.getName(), this.tablename);
  			List<FieldSchema> columns = hiveMetastoreClient.getFields(database.getName(),table.getTableName());
  			
    			for(FieldSchema column:columns)
    			{
    				
    		        colnames.add(column.getName()); 
    			}
	    }
		catch (Exception e){
			e.printStackTrace();
		}
  }  
	
	
	public List<String> getColumnNames() {
		return this.colnames;
	}
	
    
    
    public void listColumnNames() {
    	System.out.println("The Hive column names of table" + this.tablename + " of DB " + this.hivedbname + " are ");
    	for (String c: colnames) {
    		System.out.println(c);
    	}
    }
}
