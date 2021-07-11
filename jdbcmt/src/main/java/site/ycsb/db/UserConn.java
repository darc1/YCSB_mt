
package site.ycsb.db;
import java.sql.Connection;

/**
 *User conn class.
 * */
public final class UserConn {
  private String user;
  private Connection conn;

  UserConn(String user, Connection conn) {
    this.user = user;
    this.conn = conn;
  }

  Connection getConnection() {
    return this.conn;
  }

  String getUser() {
    return this.user;
  }
}
