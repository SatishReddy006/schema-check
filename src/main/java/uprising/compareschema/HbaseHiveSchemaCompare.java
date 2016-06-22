package uprising.compareschema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.google.gson.Gson;

import uprising.hbase.HBaseColumns;
import uprising.hbase.HBaseConfig;
import uprising.hbase.HBaseTables;
import uprising.hive.HiveColumns;
import uprising.hive.HiveConfig;
import uprising.hive.HiveSchemas;
import uprising.hive.HiveTables;

import org.json.simple.JSONObject;

import com.google.gson.Gson;

public class HbaseHiveSchemaCompare {
	
	public static HBaseConfig hbaseconf = null;
	public static HiveConfig hiveconf = null;
	public List<String> sourcenames;
	public String hbaseprefix;
	public String hiveprefix;
	public SchemaCheck schemaresponse = null;
	
	public HbaseHiveSchemaCompare(String hb, String hv, List<String> sourcenames){
		this.hbaseprefix = hb;
		this.hiveprefix = hv;
		this.sourcenames = sourcenames;
	}
    
	
    public String CompareHbaseHiveSchema() throws IOException {

    	String hbasepattern;
    	String hivepattern;
    	String hbtblpattern = null;
    	String hvtblpattern = null;
        String resultTableName = "BEAM.SCHEMA.CHECK";
                	    
        hbaseconf = new HBaseConfig();
        hiveconf = new HiveConfig();
	    schemaresponse = new SchemaCheck();
	    
	    for (String sourcename: this.sourcenames) 
	    {
	        HBaseTables hbaseTables = new HBaseTables(); 
	        hbaseTables.setHBaseTablesParam(hbaseconf.getConnAdmin(), hbaseconf.getConnConf());
			hbasepattern = hbaseprefix+sourcename;
			hbaseTables.setPattern(hbasepattern);
			hbaseTables.fillTableAndSchemaNames();
	        
	        HiveSchemas hiveSchemas = new HiveSchemas();
	        hiveSchemas.setHiveSchemasParam(hiveconf.getConnMetaStore(), hiveconf.getConnConf());
	        hivepattern = hiveprefix+sourcename;  
	        hiveSchemas.setPattern(hivepattern);
	        hiveSchemas.fillDBandSchemaNames();
	        
	        NameComparer nc_schema = new NameComparer();
	        nc_schema.compareNames(hbaseTables.getSchemaNames(),hiveSchemas.getSchemaNames(), "HBASE");
	    	nc_schema.compareNames(hiveSchemas.getSchemaNames(),hbaseTables.getSchemaNames(), "HIVE");
	        
	    	/*
	        System.out.println("Listing all the matched and unmatched schemas ");
	        nc_schema.listMatchItems();
	        nc_schema.listUnMatchItems("HBASE");
	        nc_schema.listUnMatchItems("HIVE");
	        */
	    	
	        List<String> schemas = nc_schema.getMatchedList();
	        
	        for (String schema: schemas){ 
	        	
		        hbtblpattern = hbaseprefix+sourcename+"."+schema+"."; 
		        hvtblpattern = hiveprefix+sourcename+"_"+schema; 
	            
		        hbaseTables.setTablePattern(hbtblpattern);
			    hbaseTables.fillTableNames();
	        	
			    HiveTables hiveTables = new HiveTables();
		        hiveTables.setHiveTablesParam(hiveconf.getConnMetaStore(), hiveconf.getConnConf());
		        hiveTables.setPattern(hvtblpattern);
		        hiveTables.fillTableNames();
	 
		        NameComparer nc_table = new NameComparer();
		        
		        nc_table.compareNames(hbaseTables.getShortTableNames(), hiveTables.getTableNames(), "HBASE");
		        nc_table.compareNames(hiveTables.getTableNames(),hbaseTables.getShortTableNames(), "HIVE");    	   	
		        
		        /*
		        System.out.println("Listing all the matched and unmatched tables ");
		        nc_table.listMatchItems();
		        nc_table.listUnMatchItems("HBASE");
		        nc_table.listUnMatchItems("HIVE");
		        */
		        
		        fillSchemaCheckTblInfo(nc_table, sourcename, schema, "HBASE");
		        fillSchemaCheckTblInfo(nc_table, sourcename, schema, "HIVE");

		        List<String> tables = nc_table.getMatchedList();
	        	
	        	for (String table: tables) {
		        	String hbcolpattern = null;
		        	String hvcolpattern = null;
		        	String hbcfpattern = null;
		        	
		        	hbcolpattern = hbaseprefix+sourcename+"."+schema+"."+table; 
	        		hvcolpattern = hiveprefix+sourcename+"_"+schema; 
		        		        		
	        		HBaseColumns hbColumns = new HBaseColumns();
	     			hbColumns.setHBaseColumnsParam(hbaseconf.getConnAdmin(), hbaseconf.getConnConf());
	        		hbColumns.setPattern(hbcolpattern, "cf1"); 
	        		hbColumns.fillColumnNames();
	        		        		
       	    	 	HiveColumns hvColumns = new HiveColumns();
	        		hvColumns.setHiveColumnsParam(hiveconf.getConnMetaStore(), hiveconf.getConnConf());
	        		hvColumns.setPattern(hvcolpattern, table); 
	        		hvColumns.fillColumnNames();    
	        			   
	        		NameComparer nc_column = new NameComparer();
    			    nc_column.compareNames(hbColumns.getColumnNames(), hvColumns.getColumnNames(), "HBASE");
    			    nc_column.compareNames(hvColumns.getColumnNames(), hbColumns.getColumnNames(), "HIVE");
    			    
    			    /*
    			    System.out.println(table);
       			    System.out.println("Listing all the matched and unmatched columns ");
       		        nc_column.listMatchItems();
       		        nc_column.listUnMatchItems("HBASE");
       		        nc_column.listUnMatchItems("HIVE");
       		        */
    			    
			        fillSchemaCheckColInfo(nc_column, sourcename, schema, table, "HBASE"); 
			        fillSchemaCheckColInfo(nc_column, sourcename, schema, table, "HIVE");    	   		     
	        	}
	        }
	    }
	    
	    
	    ResultHBaseTable rstTblinfo = new ResultHBaseTable();
	    rstTblinfo.setTableName(resultTableName);
	    rstTblinfo.setResultHBaseTableParam(hbaseconf.getConnAdmin(), hbaseconf.getConnConf());
	    rstTblinfo.createResTable();
	    rstTblinfo.fillTableinfo(schemaresponse);
	        
       
	    hbaseconf.closeConnAdmin();
	    
	    Gson gson = new Gson();
        
       return gson.toJson(schemaresponse);
	    
    }
    
    
    private void fillSchemaCheckTblInfo(NameComparer nc, String source, String schema, String base){ 
    	List<String> tmpList = null;

	    if (base.equalsIgnoreCase("HBASE")){
	    	tmpList = nc.getUnmatchedList("HBASE");
	    }
	    
	    if (base.equalsIgnoreCase("HIVE")){
	    	tmpList = nc.getUnmatchedList("HIVE");

	    }
	    
   		for (String trhb: tmpList){
   		     Table hbt = new Table(source, schema);
   	      	 hbt.fillTableName(trhb);
   	      	 schemaresponse.fillSchemacheckTable(hbt, base);
      	}
    }
    
    
    
    private void fillSchemaCheckColInfo(NameComparer nc, String source, String schema, String table, String base){
    		List<String> tmpList = null;
    		
    		List<String> matchedcolumns= new ArrayList<String>();
    		
    		matchedcolumns.add("source");
    		matchedcolumns.add("source_name");
    		matchedcolumns.add("schema");
    		matchedcolumns.add("schema_name");
    		matchedcolumns.add("row_key");
    		
		    if (base.equalsIgnoreCase("HBASE")){
		    	tmpList = nc.getUnmatchedList("HBASE");
		    	
		    }
		    
		    if (base.equalsIgnoreCase("HIVE")){
		    	tmpList = nc.getUnmatchedList("HIVE");
		    }
		    
		    tmpList.removeAll(matchedcolumns);
		    
		    for (String rhb: tmpList){
	   		     Column hbt = new Column(source, schema, table);
	   	     	 hbt.fillColumnName(rhb);
	   	     	 schemaresponse.fillSchemacheckColumn(hbt, base);
    	    }
    }
}