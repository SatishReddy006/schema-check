package uprising.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveConfig {
	
	private HiveConf hiveConf = null;
	public HiveMetaStoreClient hiveMetastoreClient = null;

	
	public HiveConfig() throws IOException
	{
		
		
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
		//System.setProperty("java.security.krb5.conf", "place/krb5/path/in/app/server");
		System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
		
		
		
		hiveConf=new HiveConf();
		hiveConf.addResource("src/main/resources/hive-site.xml");
		hiveConf.addResource("src/main/resources/core-site.xml");
		hiveConf.addResource("src/main/resources/hdfs-site.xml");
		hiveConf.set("hadoop.security.authentication", "kerberos");
		hiveConf.set("hive.server2.authentication","kerberos");
		hiveConf.set("hbase.master.kerberos.principal", "add keytab name");
		//hiveConf.addResource(new Path(System.getenv("HIVE_CONF_DIR"),"hive-site.xml"));
    	
		UserGroupInformation.setConfiguration(hiveConf);
		//UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("kerberos principal", "/place/keytab path/in/app/server");		
		UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("kerberos principal", "src/main/resources/keytab name");	
		UserGroupInformation.setLoginUser(userGroupInformation);

		try {
		     hiveMetastoreClient = new HiveMetaStoreClient(hiveConf);
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	

    
    public HiveMetaStoreClient getConnMetaStore() {
    	return this.hiveMetastoreClient;
    }
    
    
    public Configuration getConnConf() {
    	return this.hiveConf;
    }

}
