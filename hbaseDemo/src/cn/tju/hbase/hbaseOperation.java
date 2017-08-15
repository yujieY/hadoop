package cn.tju.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class hbaseOperation {

	static Configuration conf;

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		hbaseDemo hd = new hbaseDemo();
		hbaseOperation ho = new hbaseOperation();
		conf = hd.getHbaseConenct();
		// 全表扫描
		// ho.totalScan();
		// 创建表
//		ho.createHbaseTable();
		//获取所有表信息
//		ho.getHtableInfo();
		//插入数据
//		ho.insertHbaseData();
		//查询数据
		ho.getHbaseRecord();
	}

	/**
	 * 全表扫描
	 * 
	 * @throws IOException
	 * */
	public void totalScan() throws IOException {
		Scan scan = new Scan();
		System.out.println("请输入全表扫描表名：");
		Scanner sc = new Scanner(System.in);
		HTable table = new HTable(conf, sc.next().getBytes());
		ResultScanner result = table.getScanner(scan);
		Result res = result.next();
		while (res != null) {
			List<KeyValue> list = res.list();
			for (KeyValue kv : list) {
				System.out.println("key:" + kv.keyToString(kv.getKey()) + "\t"
						+ "value:" + new String(kv.getValue()));
			}
			res = result.next();
		}
	}

	/**
	 * 删除数据
	 * 
	 * @throws IOException
	 * */
	public void deleteData() throws IOException {
		System.out.println("请输入删除数据表名：");
		Scanner sc = new Scanner(System.in);
		HTable table = new HTable(conf, sc.next().getBytes());
		System.out.println("请输入删除数据行键：");
		Scanner sca = new Scanner(System.in);
		Delete del = new Delete(sca.next().getBytes());
		table.delete(del);
	}

	/**
	 * 创建表
	 * 
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 * */
	public void createHbaseTable() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		System.out.println("请输入创建表名：");
		Scanner sc = new Scanner(System.in);
		String str = sc.next();
		if (admin.tableExists(str.getBytes())) {
			admin.disableTable(str.getBytes());
			System.out.println("该视图或表已离线！");
			admin.deleteTable(str.getBytes());
			System.out.println("该视图或表已删除！");
		}
		HTableDescriptor tableDes = new HTableDescriptor(str.getBytes());
		System.out.println("请输入创建表列族（逗号分隔）：");
		Scanner sca = new Scanner(System.in);
		String s = sca.nextLine();
		String[] ss = s.split(",");
		for (int i = 0; i < ss.length; i++) {
			HColumnDescriptor col = new HColumnDescriptor(ss[i].getBytes());
			tableDes.addFamily(col);
		}
		admin.createTable(tableDes);
		System.out.println("成功创建" + str + "表！");
	}

	/**
	 * 插入数据
	 * 
	 * @throws IOException
	 * */
	public void insertHbaseData() throws IOException {
//		System.out.println("请输入待插入数据表名：");
//		Scanner sc = new Scanner(System.in);
		HTable table = new HTable(conf, "student_info".getBytes());
//		System.out.println("请输入插入数据(字段以逗号分隔)：");
//		Scanner sca = new Scanner(System.in);
//		String s = sca.nextLine();
//		String[] ss = s.split(",");
//		Put put = new Put(ss[0].getBytes());
		Put put = new Put("2016218055".getBytes());
		put.add("basicInfo".getBytes(),"age".getBytes(),"24".getBytes());
//		put.add("extralInfo".getBytes(),"hobby".getBytes(),"runing".getBytes());
		table.put(put);
		System.out.println("数据插入成功！");
	}

	/**
	 * 获取所有表信息
	 * */
	public List<String> getHtableInfo() {
		List<String> tables = null;
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin != null) {
				HTableDescriptor[] allTable = admin.listTables();
				if (allTable.length > 0) {
					tables = new ArrayList<String>();
					for (HTableDescriptor allTab : allTable) {
						tables.add(allTab.getNameAsString());
						System.out.println(allTab.getNameAsString());
					}
				}
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tables;
	}
	
	/**
	 * 查询表中记录
	 * */
	public void getHbaseRecord(){
		System.out.println("请输入查询表名：");
		Scanner sc = new Scanner(System.in);
		System.out.println("请输入查询行键：");
		Scanner sca = new Scanner(System.in);
		try {
			HTable htable = new HTable(conf,sc.next() );
			Get get =new Get(sca.next().getBytes());
			Result res = htable.get(get);
			List<KeyValue> list = res.list();
			for(KeyValue kv:list){
				System.out.print(kv.keyToString(kv.getKey())+"\t");
				System.out.println(new String(kv.getValue(),"GBK"));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
