package cn.tju.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class hiveDemo {
	public Connection getHiveConnection(String s) throws ClassNotFoundException, SQLException{
		Connection con = null;
		
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		con = DriverManager.getConnection(
				"jdbc:hive2://172.24.68.100:10000/"+s,"hadoop","hadoop");
		return con;
	}
}
