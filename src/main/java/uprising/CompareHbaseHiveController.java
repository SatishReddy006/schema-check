package uprising;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import uprising.compareschema.HbaseHiveSchemaCompare;

import org.json.simple.JSONObject;


@RestController
public class CompareHbaseHiveController {
	String  response;
	
    @RequestMapping(value = "/compare", method = RequestMethod.GET)
    public ResponseEntity<String> get() {
        return new ResponseEntity<String>("Got GET for compare", HttpStatus.OK);
    }
    
    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/compare", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE,produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> update(@RequestBody JSONObject jsonObject) throws Exception { 
    	String hbprefix = (String) jsonObject.get("hbasePrefix");
    	String hvprefix = (String) jsonObject.get("hivePrefix");
		ArrayList<JSONObject> sourcename = (ArrayList<JSONObject>)jsonObject.get("sources");
        List<String> snameList = new ArrayList<String> ();
        Iterator itr=sourcename.iterator();
        while(itr.hasNext())
    	{   
        	Map data= new LinkedHashMap();            
        	data=(LinkedHashMap)itr.next();
        	org.json.JSONObject source = new org.json.JSONObject(data);
    		String snin = (String) source.get("name");
    		snameList.add(snin);
    	}
        
        HbaseHiveSchemaCompare compareSchema = new HbaseHiveSchemaCompare(hbprefix, hvprefix, snameList);
        response = compareSchema.CompareHbaseHiveSchema();
            
        return new ResponseEntity<String>(response, HttpStatus.OK);
    }
}