package uprising.hbase;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class HBaseConfig {

    private final Configuration hbaseconfig;
    public HBaseAdmin admin = null;
    
        
    public HBaseConfig() throws IOException  {
    	
    	
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
		//System.setProperty("java.security.krb5.conf", "/place/path/here");
		System.setProperty("java.security.krb5.conf", "/place/path/here");

        hbaseconfig = HBaseConfiguration.create();
        hbaseconfig.set("hadoop.security.authentication", "kerberos");
		hbaseconfig.set("hbase.security.authentication","kerberos");
		hbaseconfig.set("hbase.zookeeper.quorum", "add zookeeper hosts");
		hbaseconfig.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseconfig.set("zookeeper.znode.parent", "/hbase-secure");
		//hbaseconfig.addResource(new Path(System.getenv("HBASE_CONF_DIR"),"hbase-site.xml")); 
        hbaseconfig.addResource("src/main/resources/hbase-site.xml");
        hbaseconfig.addResource("src/main/resources/core-site.xml");
        hbaseconfig.addResource("src/main/resources/hdfs-site.xml");
    	
        UserGroupInformation.setConfiguration(hbaseconfig);
		//UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("kerberos principal", "/place/keytab path/in/app/server");		
		UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("kerberos principal", "src/main/resources/keytab name");		
		UserGroupInformation.setLoginUser(userGroupInformation);
		
        try {
          admin=new HBaseAdmin(hbaseconfig);
        } catch (Exception e){
        	e.printStackTrace();
        }
    }    
    
    
    public HBaseAdmin getConnAdmin() {
    	return this.admin;
    }
    
    
    public Configuration getConnConf() {
    	return this.hbaseconfig;
    }
    
    
    public void closeConnAdmin() {
    	try {
    		admin.close();
    	} catch (Exception e){ 
    		e.printStackTrace();
    	}
    }
}

