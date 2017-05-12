package mdd;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.util.GenericOptionsParser;

public class Mddtj 
{
  public static class VkMapper extends Mapper<Object, Text, Text, IntWritable>
  {
    private IntWritable num = new IntWritable();
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	String line = value.toString();
    	String[] strsz = line.split(",");
    	
    	Integer n = Integer.valueOf(strsz[strsz.length - 1]);
    	num.set(n);    	
    	
    	String keyCityChannelYearAPP = "";
    	String keyCity = strsz[0];
    	String keyChannel = strsz[1];
    	String keyYear = strsz[2];
    	String keyAPP= strsz[3];
    	String keyCityChannel = keyCity + "-" + keyChannel;
    	for (int i = 0; i < strsz.length - 2; i++)  // 最后一个是num，所以是减去2不是减去1
    	{
    		keyCityChannelYearAPP += strsz[i];
    		keyCityChannelYearAPP += "-";
    	}
    	word.set(keyCityChannelYearAPP);    	// 按城市 渠道 年月 APP 统计下载量
    	context.write(word, num);
    	word.set(keyCity);      	            // 按城市合计
    	context.write(word, num);
    	word.set(keyChannel);     	            // 按渠道合计
    	context.write(word, num);
    	word.set(keyYear);     	                // 按年月合计
    	context.write(word, num);
    	word.set(keyAPP);       	            // 按APP
    	context.write(word, num);
    	
    	word.set(keyCityChannel);               // 按城市+渠道
    	context.write(word, num);
    	
    	// System.out.println("word = " + word.toString() + "  " + "num = " + num.toString());  	
    }
  }
  
  public static class VkReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();
	private MultipleOutputs<Text,IntWritable> mos;
	
	public void setup(Context context) throws IOException,InterruptedException 
	{
	   mos = new MultipleOutputs<Text,IntWritable> (context);
	   super.setup(context);
	}
	
	public void cleanup(Context context) throws IOException,InterruptedException 
	{
	   mos.close();
	   super.cleanup(context);
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
      int sum = 0;
      for (IntWritable val : values) 
      {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);    // 生成一份总的统计结果	    
      // System.out.println("reduce: key = " + key.toString() + "  " + "sum = " + result.toString());
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
    Job job = Job.getInstance(conf, "MobileDistribution");
    job.setJarByClass(Mddtj.class);
    job.setMapperClass(VkMapper.class);
    job.setCombinerClass(VkReducer.class);
    job.setReducerClass(VkReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);	
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));        
	FileSystem fs = FileSystem.get(conf);	
	Path p = new Path(otherArgs[1]);
	if (fs.exists(p))   
	{
		fs.delete(p,true);
	}
	fs.close();
    
	FileOutputFormat.setOutputPath(job, p); 	
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

