
package site.ycsb;
import site.ycsb.mt.*;
import site.ycsb.measurements.*;
import java.util.*;
import org.apache.htrace.core.Tracer;

/**
 *tenants loader class.
 * */
public final class TenantsClient {

  private TenantsClient() {
    //not used
  }
  

  public static void main(String[] args){
    System.out.println("inside tenants client");
    Properties props = Client.parseArguments(args);
    String dbname = props.getProperty(Client.DB_PROPERTY, "site.ycsb.jdbcmt.JdbcMtDBClient");
    System.out.println("dbname: " + dbname);

    props.setProperty(TenantManager.MULTI_TENANT_INIT, String.valueOf(true));
    Measurements.setProperties(props);
    final Tracer tracer =  new Tracer.Builder("YCSB Tenants loader")
        .conf(Client.getHTraceConfiguration(props))
        .build();
    DB db;
    try {
      db = DBFactory.newDB(dbname, props, tracer);
      db.init();
    } catch (UnknownDBException e) {
      System.out.println("Unknown DB " + dbname);
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(1);
    }catch(DBException e){
      System.out.println("Failed to init db " + dbname);
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(1);
    }
    
  }
}
