package com.natty.Hdsetest.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.CustomDataWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PhoneStatisticsMapReduce extends Configured implements Tool{
	
	
	public static class PhoneStatisticsMapper 
	extends Mapper<LongWritable, Text, LongWritable, CustomDataWritable> {
		
	private LongWritable phoneno = new LongWritable();
	private CustomDataWritable datatool = new CustomDataWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO 
		
		//如果手机号为空的话，不统计。
		String[] strs = value.toString().split("\t");
		if(StringUtils.isBlank(strs[1]) || !StringUtils.isNumeric(strs[1])){
			return ;
		}
		
		//确保这个几个字段都是数字的记录才统计。
		if(  
			 !StringUtils.isNumeric(strs[6]) ||
			 !StringUtils.isNumeric(strs[7]) ||
			 !StringUtils.isNumeric(strs[8]) ||
			 !StringUtils.isNumeric(strs[9]) 				
				){
			return;
		}
		else
		{
			phoneno.set(Long.valueOf(strs[1]));
			datatool.setUpPackNum(Integer.valueOf(strs[6]));
			datatool.setDownPackNum(Integer.valueOf(strs[7]));
			datatool.setUpPayLoad(Integer.valueOf(strs[8]));
			datatool.setDownPayLoad(Integer.valueOf(strs[9]));
			datatool.setCountNum(1);
			context.write(phoneno, datatool);
		}
	}
}

public static class PhoneStatisticsReducer 
	extends Reducer<LongWritable, CustomDataWritable, LongWritable, CustomDataWritable> {
	
	private CustomDataWritable datatool = new CustomDataWritable();
	
	@Override
	protected void reduce(LongWritable key, Iterable<CustomDataWritable> values,Context context) 
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int UpPackNumSum = 0;
		int DownPackNumSum = 0;
		int UpPayLoadSum = 0;
		int DownPayLoadSum = 0;
		int CountNumSum = 0 ;
		
		//聚合 同一个key的所有value值，SUM计算方法。
		for(CustomDataWritable value: values){
			UpPackNumSum += value.getUpPackNum();
			DownPackNumSum += value.getDownPackNum();
			UpPayLoadSum += value.getUpPayLoad();
			DownPayLoadSum += value.getDownPayLoad();
			CountNumSum += value.getCountNum();	
		}
		datatool.setUpPackNum(UpPackNumSum);
		datatool.setDownPackNum(DownPackNumSum);
		datatool.setUpPayLoad(UpPayLoadSum);
		datatool.setDownPayLoad(DownPayLoadSum);
		datatool.setCountNum(CountNumSum);
		
		context.write(key, datatool);
	}	
}

public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	//获取配置文件：
	Configuration conf = super.getConf();
	
	//创建job：
	Job job = Job.getInstance(conf, this.getClass().getSimpleName());
	job.setJarByClass(PhoneStatisticsMapReduce.class);
	
	//配置作业：
	// Input --> Map --> Reduce --> Output
	// Input:
	Path inPath = new Path(args[0]);
	FileInputFormat.addInputPath(job, inPath);      
			//FileInputFormat过程会将文件处理（Format）成 <偏移量,每一行内容> 的key value对。
	
	//Map  设置Mapper类，设置Mapper类输出的Key、Value的类型：
	job.setMapperClass(PhoneStatisticsMapper.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(CustomDataWritable.class);
	
	//Reduce  设置Reducer类， 设置最终输出的 Key、Value的类型（setOutputKeyClass、setOutputValueClass）： 
	job.setReducerClass(PhoneStatisticsReducer.class);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(CustomDataWritable.class);
	
	//Output 设置输出路径
	Path outPath =  new Path(args[1]);
	FileOutputFormat.setOutputPath(job, outPath);
	
	//提交任务
	boolean isSucess = job.waitForCompletion(true);
	return isSucess ? 1:0;     //成功返回1 ，失败返回0
}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int status = ToolRunner.run(conf, new PhoneStatisticsMapReduce(), args) ;
		System.exit(status);
	}
}

/**
 * 
 * 上传文件到HDFS：
 * bin/hdfs dfs -put /home/natty.ma/bigdata/hadoop/files/phoneStatistic/HTTP_20130313143750.data /user/natty.ma/mapreduce/files/
 * bin/hdfs dfs -text /user/natty.ma/mapreduce/files/HTTP_20130313143750.data 
 * 
 * 执行mapreduce：
 * hadoop jar PhoneStatisticsMR.jar com.natty.Hdsetest.mapreduce.PhoneStatisticsMapReduce /user/natty.ma/mapreduce/files/HTTP_20130313143750.data /user/natty.ma/hdfsapi/output10
 * 
 * 查看运行结果：
 * bin/hdfs dfs -text /user/natty.ma/hdfsapi/output10/part*
 * 
 * 
 * 84138413        20      16      4116    1432    1
 * 13480253104     3       3       180     180     1
 * 13502468823     57      102     7335    110349  1
 * 13560439658     33      24      2034    5892    2
 * 13600217502     18      138     1080    186852  1
 * 13602846565     15      12      1938    2910    1
 * 13660577991     24      9       6960    690     1
 * 13719199419     4       0       240     0       1
 * 13726230503     24      27      2481    24681   1
 * 13760778710     2       2       120     120     1
 * 13823070001     6       3       360     180     1
 * 13826544101     4       0       264     0       1
 * 13922314466     12      12      3008    3720    1
 * 13925057413     69      63      11058   48243   1
 * 13926251106     4       0       240     0       1
 * 13926435656     2       4       132     1512    1
 * 15013685858     28      27      3659    3538    1
 * 15920133257     20      20      3156    2936    1
 * 15989002119     3       3       1938    180     1
 * 18211575961     15      12      1527    2106    1
 * 18320173382     21      18      9531    2412    1
 * 
 */

