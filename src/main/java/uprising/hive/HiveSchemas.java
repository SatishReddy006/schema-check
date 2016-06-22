package uprising.hive;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;



public class HiveSchemas {
	HiveMetaStoreClient hiveMetastoreClient = null;
	Configuration hconf = null;
	String pattern = null;
	List<String> hvdbnames = null;
	List<String> hvschnames = null;

	public HiveSchemas(){
		hvdbnames = new ArrayList<String> ();
		hvschnames = new ArrayList<String> ();
	}
	
	
	public HiveSchemas(String ptr){
		this.pattern = ptr;
		hvdbnames = new ArrayList<String> ();
		hvschnames = new ArrayList<String> ();
	}
	
	public void setPattern(String ptr){
		this.pattern = null;
		this.pattern = ptr;
	}
	
	public void setHiveSchemasParam(HiveMetaStoreClient hclient, Configuration conf){
		this.hiveMetastoreClient = hclient;
		this.hconf = conf;
	}
	
	public void fillDBNames() throws IOException {
		try {
    	    List<String> dbnames = hiveMetastoreClient.getAllDatabases();

    		for(String db:dbnames)
    		{
    			if (db.startsWith(this.pattern.toLowerCase())) hvdbnames.add(db);
    		}
	    }
		catch (Exception e){
			e.printStackTrace();
		}
  } 
	
	
	public void fillDBandSchemaNames() throws IOException {
		try {
    	    List<String> dbnames = hiveMetastoreClient.getAllDatabases();

    		for(String db:dbnames)
    		{
    			hvdbnames.add(db);
    			if (db.startsWith(this.pattern.toLowerCase())) {
    				String string=db.replaceFirst(this.pattern.toLowerCase(),"");
    				if(string.startsWith("_"))
    				{
    					hvschnames.add(string.replaceFirst("_",""));
    				}
    				else
    				{
    					hvschnames.add(string);
    				}
    				/*String[] res_s = db.split("_");
    				if (res_s.length == 2) { 
    					hvdbnames.add(db);
    					hvschnames.add(res_s[1]);
    				}
    				if (res_s.length > 2) { 
    					hvdbnames.add(db);
    					hvschnames.add(StringUtils.substringAfter(db, "_"));
    				}*/
    			}
    		}
	    }
		catch (Exception e){
			e.printStackTrace();
		}
  } 
	
	
	public List<String> getDBNames() {
		return hvdbnames;
	}
	
    
    public void listDBNames() {
    	System.out.println("The Hive DB names are ");
    	for (String s:hvdbnames){
    		System.out.println(s);
    	}
    }
    
	
	public List<String> getSchemaNames() {
		return hvschnames;
	}
	
    public void listSchemaNames() {
    	System.out.println("The Hive Schema names are ");
    	for (String s:hvschnames){
    		System.out.println(s);
    	}
    }
}
