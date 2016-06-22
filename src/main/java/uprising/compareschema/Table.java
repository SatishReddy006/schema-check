package uprising.compareschema;

class Table {
    private String source;
    private String schema;
    private String table;
    
    
    public Table(String src, String sch) {
    	source = src;
    	schema = sch;
    }
    
    
    public void fillTableInfo(String src, String sch, String name){
    	source = src;
    	schema = sch;
    	table = name;
    	
    }
    
    
    public void fillTableName(String name){
    	table = name;
    	
    }
    
    
    
    public String getSourcename() {
    	return source;
    }
    
    
    public String getSchemaname() {
    	return schema;
    }
    
    public String getTablename() {
    	return table;
    }
    
    public void listTableName(){
    	System.out.println("Table Name ::"+table);
    }
}