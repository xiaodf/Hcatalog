package iie.hadoop.hcatalog;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 读取HDFS文件实现WordCount示例
 * 
 * 传入参数为config.properties文件，指定输入、输出文件地址及名字
 * 例：输入文件内容为：“张三 李四 张三 李四”
 * 输出文件内容为： 张三	2
 *                李四	2
 * 
 * 执行命令：hadoop jar spark.jar config.properties
 * @author xiaodongfang
 *
 */
public class TextFileMRTest {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();// Text key

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String inputFile = null;
		String outputFile = null;
		try {
			// 获取配置文件参数
			String confFile = args[0];
			File configFile = new File(confFile);
			System.out.println("configuration file is:" + confFile);
			InputStream in = new FileInputStream(configFile);
			Properties props = new Properties();
			props.load(in);

			inputFile = props.getProperty("inputFile");
			outputFile = props.getProperty("outputFile");
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Job job = Job.getInstance(conf);
		job.setJobName("word count");
		job.setJarByClass(TextFileMRTest.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// 指定输入文件路径及名字
		FileInputFormat.addInputPath(job, new Path(inputFile));
		// 指定输出文件路径及名字
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		System.out.println("Output 'inputFile' is:" + inputFile);
		System.out.println("Output 'outputFile' is:" + outputFile);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
