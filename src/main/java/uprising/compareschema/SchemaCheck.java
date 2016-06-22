package uprising.compareschema;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.Gson;
import org.json.simple.JSONObject;

public class SchemaCheck {
    private String checkId;
    private Date checkTime;
    private List<Table>  missingHBaseTables;
    private List<Table> missingHiveTables;
    private List<Column> missingHBaseColumns;
    private List<Column> missingHiveColumns;
    String delimiter = null;
    
    public SchemaCheck(){
    	checkTime = new java.util.Date();
		String processName =   java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
		checkId = String.valueOf(Long.parseLong(processName.split("@")[0]));
		missingHBaseTables = new ArrayList<Table> ();
		missingHiveTables = new ArrayList<Table> ();
		missingHBaseColumns = new ArrayList<Column> ();
		missingHiveColumns = new ArrayList<Column> ();
    }
       
   
    public String getPid() {
    	return checkId;
    }
        
    public String getTimeStamp() {
		Timestamp t = new Timestamp(checkTime.getTime());
		String timeStamp = t.toString();
		return timeStamp;
    }
      
    public void fillSchemacheckTable(Table tbl, String base){
    	if (base.equalsIgnoreCase("HIVE")){
    		missingHBaseTables.add(tbl);
      	}
    	if (base.equalsIgnoreCase("HBASE")){
    		missingHiveTables.add(tbl);
       	}
    }
    
    
    
    public void fillSchemacheckColumn(Column col, String base){
    	if (base.equalsIgnoreCase("HIVE")){
    		missingHiveColumns.add(col);
       	}
    	if (base.equalsIgnoreCase("HBASE")){
    		missingHBaseColumns.add(col);
       	}
    }
           
    @SuppressWarnings("unchecked")
	public String getJStringSchemaObject(){
         
        JSONObject strjson = new JSONObject();

        try {
	           	
        	JSONObject jsonobj = new JSONObject();
        	Gson gson = new Gson();
        	
        	jsonobj.put("Hbase Tables Doesn't exist in Hive", gson.toJson(missingHBaseTables));
        	jsonobj.put("Hive Tables Doesn't exist in HBase", gson.toJson(missingHiveTables));
        	jsonobj.put("HBase Columns Doesn't exist in Hive", gson.toJson(missingHBaseColumns));
        	jsonobj.put("Hive Columns Doesn't exist in HBase",gson.toJson(missingHiveColumns));
        	       
	        strjson.put("Discrepancies", jsonobj);
        }
        catch (Exception e) {
        	e.printStackTrace();
        }
        
        return strjson.toString();
    }
    
}