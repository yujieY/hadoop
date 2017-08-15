package cn.tju.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class hiveOperation {

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		// TODO Auto-generated method stub
		Connection con = null;
		Statement st = null;
		ResultSet res = null;
		int count = 0;
		String flag = "false";
		String str ="";

		hiveDemo hd = new hiveDemo();
		System.out.println("请输入需要连接的数据库：");
		Scanner sc = new Scanner(System.in);
		String s = sc.next();
		con = hd.getHiveConnection(s);
		String sql = "";
		System.out.println("请输入Hive数据库操作：");
		Scanner sca = new Scanner(System.in);
		sql = sca.nextLine();
		while (!sql.equals(flag)) {
			System.out.println("当前使用的数据库为："+s);
			System.out.println("input sql is:"+sql);
			System.out.println("-----------------------------------------------------------------------------------------");
			st = con.createStatement();
			res = st.executeQuery(sql);
			while (res.next()) {
//				System.out.println(res.getString(1));
				ResultSetMetaData rs = res.getMetaData();
				count = rs.getColumnCount();
				for (int i = count; i >0 ; i--) {
					str = res.getString(i)+"\t"+str;
				}
				System.out.println(str+"\n");
				str = "";
			}
			System.out.println("请输入Hive数据库操作(false退出)：");
			Scanner scan = new Scanner(System.in);
			sql = scan.nextLine();
		}
		if (res != null) {
			res.close();
		}
		if (st != null) {
			st.close();
		}
		if (con != null) {
			con.close();
		}
	}

}
