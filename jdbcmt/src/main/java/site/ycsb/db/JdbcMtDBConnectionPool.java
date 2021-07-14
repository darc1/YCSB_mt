package site.ycsb.db;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import site.ycsb.*;
import site.ycsb.mt.TenantManager;

/**
 * Connection Pool for multi thread.
 */
public final class JdbcMtDBConnectionPool {

  private static final String DEFAULT_PROP = "";
  private Map<String, List<UserConn>> tenantConns;
  private ConcurrentMap<String, ConcurrentMap<StatementType, PreparedStatement>> tenantCachedStatements;
  private static JdbcMtDBConnectionPool instance;
  private boolean initialized = false;
  private boolean autoCommit;
  private Random random;
  private int seed = 90901;

  public static synchronized JdbcMtDBConnectionPool instance() {
    if (instance == null) {
      instance = new JdbcMtDBConnectionPool();
    }

    return instance;
  }

  private JdbcMtDBConnectionPool() {
  }

  public synchronized void init(Properties props, TenantManager tenantManager) throws DBException {
    if (initialized) {
      return;
    }

    random = new Random(seed);

    this.autoCommit = JdbcMtDBClient.getBoolProperty(props, JdbcMtDBClient.JDBC_AUTO_COMMIT, true);
    tenantConns = new HashMap<String, List<UserConn>>();
    tenantCachedStatements = new ConcurrentHashMap<String, ConcurrentMap<StatementType, PreparedStatement>>();
    String urls = props.getProperty(JdbcMtDBClient.CONNECTION_URL, DEFAULT_PROP);
    String driver = props.getProperty(JdbcMtDBClient.DRIVER_CLASS);
    for (String tenantId : tenantManager.getTenantIds()) {
      tenantConns.put(tenantId, new ArrayList<UserConn>());
      String user = tenantManager.getTenantUsers(tenantId)
          .get(random.nextInt(tenantManager.getTenantUsers(tenantId).size()));
      List<Connection> dbConns = createConnection(urls, user, JdbcMtDBClient.DEFAULT_USERS_PASSWORD, driver);
      List<UserConn> userConns = new ArrayList<>();
      for (Connection conn : dbConns) {
        userConns.add(new UserConn(user, conn));
      }
      tenantCachedStatements.put(user, new ConcurrentHashMap<>());
      tenantConns.get(tenantId).addAll(userConns);
    }

    initialized = true;
  }

  public Map<String, List<UserConn>> getTenantConns() {
    return this.tenantConns;
  }

  public ConcurrentMap<String, ConcurrentMap<StatementType, PreparedStatement>> getTenantCachedStatements() {
    ConcurrentMap<String, ConcurrentMap<StatementType, PreparedStatement>> result = new ConcurrentHashMap<>();
    for (String user : this.tenantCachedStatements.keySet()) {
      result.put(user, new ConcurrentHashMap<>());
    }
    return result;
  }

  private List<Connection> createConnection(String urls, String user, String passwd, String driver) throws DBException {

    List<Connection> result = null;
    try {
      if (driver != null) {
        Class.forName(driver);
      }
      result = new ArrayList<Connection>(3);
      // for a longer explanation see the README.md
      // semicolons aren't present in JDBC urls, so we use them to delimit
      // multiple JDBC connections to shard across.
      final String[] urlArr = urls.split(";");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url + " user: " + user + " password: " + passwd);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        result.add(conn);
      }

    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    return result;
  }
}
