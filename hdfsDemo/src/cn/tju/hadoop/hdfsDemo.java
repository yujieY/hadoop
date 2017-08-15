package cn.tju.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class hdfsDemo {

	public FileSystem getConnection() throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://172.24.68.100:9000");
		FileSystem filesys = FileSystem.get(conf);
		return filesys;
	}

}
