package nlsde.buaa.base.configer;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

public class ConnectionManager {
	private static ConnectionManager instance;

	public ComboPooledDataSource ds;
	private final static String c3p0Properties = "conf"+File.separator+"c3p0.properties";
	
	private ConnectionManager() throws Exception {
		Properties props = new Properties();
		FileInputStream in=new FileInputStream(c3p0Properties);
		props.load(in);
		ds = new ComboPooledDataSource();
		
		ds.setDriverClass(props.getProperty("driverClass"));
		ds.setJdbcUrl(props.getProperty("jdbcUrl"));
		ds.setUser(props.getProperty("user"));
		ds.setPassword(props.getProperty("password"));
	}

	public static final ConnectionManager getInstance() {
		if (instance == null) {
			try {
				instance = new ConnectionManager();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return instance;
	}

	public synchronized final Connection getConnection() {
		try {
			return ds.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected void finalize() throws Throwable {
		DataSources.destroy(ds); // 关闭datasource
		super.finalize();
	}
}
