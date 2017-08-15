package cn.tju.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


public class hbaseDemo {
	public Configuration getHbaseConenct(){
		Configuration conf = HBaseConfiguration.create();
		return conf;
	}
	
}
