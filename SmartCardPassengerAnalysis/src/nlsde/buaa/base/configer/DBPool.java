package nlsde.buaa.base.configer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBPool {

	public static void main(String[] args) {
		DBPool.getDBConnection();
	}

	public static Connection getDBConnection() {
		//返回JDBC connection
//		return DBPool.getJDBCConnection();
		//改为连接池
		return ConnectionManager.getInstance().getConnection();
	}

	public static Connection getJDBCConnection() {
		// 驱动程序名
		String driver = "com.mysql.jdbc.Driver";

		// URL指向要访问的数据库名scutcs
		String url = "jdbc:mysql://127.0.0.1:3306/passenger";

		// MySQL配置时的用户名
		String user = "passenger";

		// MySQL配置时的密码
		String password = "itsnlsde";
		try {
			// 加载驱动程序
			Class.forName(driver);

			// 连续数据库
			Connection conn = DriverManager.getConnection(url, user, password);

			if (!conn.isClosed()) {
				System.out.println("Succeeded connecting to the Database!");
				return conn;
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
