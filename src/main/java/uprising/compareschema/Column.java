package uprising.compareschema;

import java.util.ArrayList;
import java.util.List;

class Column {
   private String source;
   private String schema;
   private String table;
   private String column;
   
   
   
   public Column(String src, String sch) {
   	source = src;
   	schema = sch;
   }
   
   
   
   public Column(String src, String sch, String tbl) {
	   	source = src;
	   	schema = sch;
	   	table = tbl;
	   }
   
  
   public void fillColumnInfo(String src, String sch, String tbl, String name){
   	source = src;
   	schema = sch;
   	table = tbl;
   	column = name;
   }
   
   
   public String getSourcename() {
   	return source;
   }
   
  
   public String getSchemaname() {
   	return schema;
   }
   
   
   public String getColumnname() {
   	return column;
   }
   
   public String getTablename() {
   	return table;
   }
   
   public void fillColumnName(String name){
		column = name;

   }
}