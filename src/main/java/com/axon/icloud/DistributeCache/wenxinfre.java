package com.axon.icloud.DistributeCache;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author zhulei
 * 对于一些配置信息，我们不要使用配置文件的，hadoop提供了分布式缓存，正好被用来加载配置文件
 * 这个mapreduce就是介绍如何使用分布式缓存
 */

public class wenxinfre {
	private static Log logger = LogFactory.getLog(wenxinfre.class);

	public static void main(String[] args) {
              Configuration conf = new Configuration();
              Job job  = null;
             try {
				job = Job.getInstance(conf);
				
				job.setJarByClass(wenxinfre.class);
				
				//添加配置文件地址
				job.addCacheFile(new URI("hdfs://10.10.141.57:9000/cache/weixintabbiz.txt"));
				
				//设置mapper
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(weixinBean.class);
				job.setMapperClass(weixinMapper.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				
				//设置mapper
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setReducerClass(wenxinfreReducer.class);
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				job.waitForCompletion(true);
				
			} catch (IOException e) {
				logger.error(e);
			} catch (URISyntaxException e) {
				logger.error(e);
			} catch (ClassNotFoundException e) {
				logger.error(e);
			} catch (InterruptedException e) {
				logger.error(e);
			}
             
	}

	public static class weixinMapper extends
			Mapper<LongWritable, Text, Text, weixinBean> {
		private HashMap<String, String> tagHm = new HashMap<String, String>();

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, weixinBean>.Context context)
				throws IOException, InterruptedException {
			logger.info("开始setup");
			super.setup(context);
			// java.net的uri
			URI[] cacheFiles = context.getCacheFiles();
			Path path = new Path(cacheFiles[0]);
			FileSystem fs = FileSystem.get(cacheFiles[0], new Configuration());
			InputStream in = fs.open(path);
			List<String> list = IOUtils.readLines(in);
		    for(String line :list){
		    	String[] str = line.split("\\s+");
		    	tagHm.put(str[0], str[1]);
		    }
			               
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, weixinBean>.Context context)
				throws IOException, InterruptedException {
			// str[tel,time,biz]
			String[] str = value.toString().split("\t");
			weixinBean wxb = new weixinBean();
			wxb.setDate(str[1]);
			String tag = tagHm.get(str[2]);
			wxb.setBiz(str[2]);
			if (null == tag) {
				wxb.setTag(-1);
			} else {
				wxb.setTag(Integer.parseInt(tag));
			}
			wxb.setDate(str[1]);
			wxb.setTel(str[0]);
			context.write(new Text(str[0]), wxb);
		}

	}

	public static class wenxinfreReducer extends
			Reducer<Text, weixinBean, Text, Text> {

		@Override
		protected void reduce(Text text, Iterable<weixinBean> values,
				Reducer<Text, weixinBean, Text, Text>.Context context)
				throws IOException, InterruptedException {
			HashMap<String,Integer> hm = new HashMap<String, Integer>();
			for(weixinBean wxb : values){
				String key = wxb.getTel()+","+wxb.getTag();
				Integer in = hm.get(key);
				if(null==in){
					hm.put(key, 1);
				}else {
					in+=1;
					hm.put(key, in);
				}
			}
		   Set<Map.Entry<String, Integer>> set = hm.entrySet();
		   for(Iterator<Map.Entry<String, Integer>> it = set.iterator();it.hasNext();){
			   Map.Entry<String, Integer> me = it.next();
			   String [] str = me.getKey().split(",");
			   int fre = me.getValue();
			  context.write(new Text(str[0]), new Text(str[1]+"\t"+fre));
		   }
		}
		

	}
}
