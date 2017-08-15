package cn.tju.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.util.BufferRecycler;

public class hdfsOperation {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		hdfsDemo hd = new hdfsDemo();
		hdfsOperation ho = new hdfsOperation();
		FileSystem fs = hd.getConnection();
		// 文件检索
//		ho.searchLine(fs);
		// 文件读取
//		ho.readFile(fs);
		//读取文件至本地
//		ho.readFileToLocal(fs);
		//上传文件至Hdfs
		ho.uploadFile(fs);
	}

	/**
	 * 文件检索
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 * */
	public void searchLine(FileSystem fs) throws FileNotFoundException,
			IllegalArgumentException, IOException {
		System.out.print("请输入检索路径：");
		Scanner input = new Scanner(System.in);
		FileStatus[] files = fs.listStatus(new Path(input.next()));
		for (FileStatus file : files) {
			System.out.println(file.getPath() + "---------------------------"
					+ (file.isDir() ? "Dir" : "File"));
		}
	}
	
	
	/**
	 * 文件读取
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * */
	public void readFile(FileSystem fs) throws IllegalArgumentException, IOException{
		System.out.print("请输入需要读取的文件路径：");
		Scanner in = new Scanner(System.in);
		FSDataInputStream input = fs.open(new Path(in.next()));
		BufferedReader br = new BufferedReader(new InputStreamReader(input));
		String line = br.readLine();
		while(line!=null){
			System.out.println(line);
			line = br.readLine();
		}
	}
	
	/**
	 * 读取文件至本地(非文件夹)
	 * @throws IOException 
	 * */
	public void readFileToLocal(FileSystem fs) throws IOException{
		System.out.print("请输入需要读取的文件路径：");
		Scanner in = new Scanner(System.in);
		FSDataInputStream input = fs.open(new Path(in.next()));
		System.out.print("请写入输入本地的文件路径：");
		Scanner inLocal = new Scanner(System.in);
		FileOutputStream fileout = new FileOutputStream(new File(inLocal.next()));
		byte[] bs = new byte[1024];
		int readLength = input.read(bs);
		while(readLength!=-1){
			fileout.write(bs,0,readLength);
			fileout.flush();
			readLength = input.read(bs);
		}
		System.out.println("文件下载成功！");
	}
	
	/**
	 * 上传文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * */
	public void uploadFile(FileSystem fs) throws IllegalArgumentException, IOException{
		System.out.print("请输入需要上传的文件路径：");
		Scanner in = new Scanner(System.in);
		File uploadFile = new File(in.next());
		FileInputStream input = new FileInputStream(uploadFile);
		System.out.print("请输入需要上传至Hdfs的文件路径：");
		Scanner inp = new Scanner(System.in);
		FSDataOutputStream out = fs.create(new Path(inp.next()));
		byte[] bs = new byte[1024];
		int readLength = input.read();
		while(readLength!=-1){
			out.write(bs);
			out.flush();
			readLength = input.read();
		}
		out.close();
		input.close();
		System.out.println("上传文件成功！");
	}
}
