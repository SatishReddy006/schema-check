package uprising.hive;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;


public class HiveTables {
	HiveMetaStoreClient hiveMetastoreClient = null;
	Configuration hconf = null;
	String hivedbname = null;
	List<String> htablenames = null;
	
	
	public HiveTables(){
		htablenames = new ArrayList<String> ();
	}
	
	
	public HiveTables(String dbname){
		this.hivedbname = dbname;
		htablenames = new ArrayList<String> ();
	}
	
	
	public void setPattern(String dbname){
		this.hivedbname = dbname;
	}
	
	
	
	public void setHiveTablesParam(HiveMetaStoreClient hclient, Configuration conf){
		this.hiveMetastoreClient = hclient;
		this.hconf = conf;
	}
	
	
	public void fillTableNames() throws IOException {
		try {
    	    Database database = hiveMetastoreClient.getDatabase(this.hivedbname);
       		List<String> tables = hiveMetastoreClient.getAllTables(database.getName());

    		for(String table:tables)
    		{
    			//System.out.println("Table "+table+" is in "+database.getName()+" database");
    			htablenames.add(table);
    		}
	    }
		catch (Exception e){
			e.printStackTrace();
		}
  } 
	
	
	public List<String> getTableNames() {
		return htablenames;
	}
	
    public void listTableNames() {
    	System.out.println("The Hive table names of DB" + this.hivedbname + " are ");
    	for (String s:htablenames){
    		System.out.println(s);
    	}
    }
}
