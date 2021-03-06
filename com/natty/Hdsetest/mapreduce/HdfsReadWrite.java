package com.natty.Hdsetest.mapreduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsReadWrite {
	
	private Configuration conf;
	private FileSystem fs;
	
	/**
	 * Read the content of the file in HDFS to the console!!!
	 * author: natty
	 * date : 2016-12-30
	 * @param path : The files or directory you want to read in hdfs .
	 * @throws IOException 
	 */
	public void readHdfsFile(Path path) 
			throws IOException{
		conf = new Configuration();
		fs = FileSystem.get(conf);	
		FSDataInputStream in = fs.open(path);
		try {
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			in.close();
			fs.close();
		}
		
	}
	
	/**
	 * copy the files from one directory(main Linux) to another(HDFS)
	 * author : natty
	 * date : 2016-12-30
	 * @param file : the local file in Linux 
	 * 		  path : the HDFS directory in cluster
	 * @throws IOException
	 * 
	 * hadoop jar hdfsAPI.jar com.beifeng.Hdsetest.HdfsReadWrite 2 /home/natty.ma/bigdata/hadoop/files/hdfsapi/core-site.xml /user/natty.ma/hdfsapi/output
	 */
	public void writeFromlocalToHdfs(File file,Path path) throws IOException{
		conf = new Configuration();
		fs = FileSystem.get(conf);
		FileInputStream ins = new FileInputStream(file);
		// 输出流的path必须是一个文件，不能是一个目录。
		FSDataOutputStream out = fs.create(path);
		
		try {
			IOUtils.copyBytes(ins, out, 4096, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			ins.close();
			out.close();
			fs.close();
		}
		
	}
	
	
	public void work(File localFile,OutputStream out) throws IOException
	{
		InputStream in = null;
		if(localFile.isFile())
		{
			int byteRead = 0;  
            byte[] buffer = new byte[256];  
            while ((byteRead = in.read(buffer)) > 0) {  
                out.write(buffer, 0, byteRead);  
            }  
		}
		else if(localFile.isDirectory())
		{
			File[] files = localFile.listFiles();
			for(int i=0;i<files.length;i++)
			{
				if(files[i].isFile())
				{
					in = new BufferedInputStream(new FileInputStream(files[i]));
					int byteRead = 0;
					byte[] buffer =  new byte[256];
					while((byteRead=in.read(buffer))>0)
					{
						out.write(buffer, 0, byteRead);
					}
				}
				else
				{
					work(files[i],out);
				}
			}
		}
	}
	
	/**
	 * Linux环境上的目录里的文件，合并到一个文件中，复制到HDFS。
	 * date: 2017-1-3
	 * @throws IOException 
	 * 
	 * hadoop jar hdfsAPI.jar com.beifeng.Hdsetest.HdfsReadWrite 3 /home/natty.ma/bigdata/hadoop/files/hdfsapi /user/natty.ma/hdfsapi/output2
	 */
	public void mergeLittleFiles(File localFile, Path hdpath) throws IOException
	{
		conf = new Configuration();
		fs = FileSystem.get(conf);
		
		FSDataOutputStream out = fs.create(hdpath);
		work(localFile,out);
	}
	
	/**
	 * 
	 * @param args  
	 * 		args[0] 三个参数，分别测试三个方法 （1,2,3）
	 * 		1: testing readHdfsFile :   args[1] : HDFS上文件路径 
	 *      2: testing writeFromlocalToHdfs : args[1] : Linux local file.   args[2]:HDFS上路径
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {		
		HdfsReadWrite hrw = new HdfsReadWrite();
		if(args[0].equals("1"))
		{
			Path path= new Path(args[1]);
			hrw.readHdfsFile(path);
		}
		
		if(args[0].equals("2"))
		{
			File file = new File(args[1]);
			Path path = new Path(args[2]);
			hrw.writeFromlocalToHdfs(file, path);
		}
		
		if(args[0].equals("3"))
		{
			File file = new File(args[1]);
			Path path = new Path(args[2]);
			hrw.mergeLittleFiles(file, path);
		}

	}
}
