package com.natty.Hdsetest.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WebPVMapReduce extends Configured implements Tool{

	public static class WebPVMapper 
	extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		private IntWritable keyPhone = new IntWritable();
		private static final IntWritable val = new IntWritable(1);
	
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO 
			String LineVal = value.toString();
			String[] strs =  LineVal.split("\t");
			
			//如果解析出的字段数少于31个，这样的记录去掉。
			if(strs.length < 31){
				return;			
			}
			
			//判断某字符串是否为空或长度为0或由空白符(whitespace) 构成 
			//如果没有对应的URL，不计算这样的PV。
			if(StringUtils.isBlank(strs[1])){
				return;
			}
			
			//如果对应的省份代码是空，不统计。
			if(StringUtils.isBlank(strs[23])){
				return;
			}
			
			//判断省份代码必须是数字。
			if(StringUtils.isNumeric(strs[23])){
				keyPhone.set(Integer.valueOf(strs[23]));
				context.write(keyPhone, val);
			}
			else {
				return;
			}
		}
}
	
	public static class WebPVReducer 
	extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		private IntWritable val = new IntWritable();	
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			val.set(sum);
			context.write(key,val);	
		}	
		
}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		//获取配置文件：
		Configuration conf = super.getConf();
		
		//创建job：
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(WebPVMapReduce.class);
		
		//配置作业：
		// Input --> Map --> Reduce --> Output
		// Input:
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);      
				//FileInputFormat过程会将文件处理（Format）成 <偏移量,每一行内容> 的key value对。
		
		//Map  设置Mapper类，设置Mapper类输出的Key、Value的类型：
		job.setMapperClass(WebPVMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//Reduce  设置Reducer类， 设置最终输出的 Key、Value的类型（setOutputKeyClass、setOutputValueClass）： 
		job.setReducerClass(WebPVReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Output 设置输出路径
		Path outPath =  new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		
		//提交任务
		boolean isSucess = job.waitForCompletion(true);
		return isSucess ? 1:0;     //成功返回1 ，失败返回0
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int status = ToolRunner.run(conf, new WebPVMapReduce(), args) ;
		System.exit(status);
	}
}

/*
 * run_demo: hadoop jar webpv.jar com.natty.Hdsetest.mapreduce.WebPVMapReduce /user/natty.ma/mapreduce/files/2015082818 /user/natty.ma/mapreduce/output1
 * output:
    1       3527
	2       1672
	3       511
	4       325
	5       776
	6       661
	7       95
	8       80
	9       183
	10      93
	11      135
	12      289
	13      264
	14      374
	15      163
	16      419
	17      306
	18      272
	19      226
	20      2861
	21      124
	22      38
	23      96
	24      100
	25      20
	26      157
	27      49
	28      21
	29      85
	30      42
	32      173 
 * 
 */


