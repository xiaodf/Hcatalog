package iie.hadoop.hcatalog;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

/**
 * 利用HCatInputFormat、HCatOutputFormat接口，从hive表里读数据，按age字段分组
 * 
 * CREATE TABLE student (name STRING，age INT) ROW FORMAT DELIMITED FIELDS
 * TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
 * 
 * CREATE TABLE student_out (age INT，count INT) ROW FORMAT DELIMITED FIELDS
 * TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
 * 
 * @author xiaodongfang
 * 
 */
public class HCatMRTest extends Configured implements Tool {

	@SuppressWarnings("rawtypes")
	public static class Map extends
			Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

		Integer age;
		HCatSchema schema;

		@Override
		protected void map(WritableComparable key, HCatRecord value,
				Context context) throws IOException, InterruptedException {
			// Get our schema from the Job object.
			schema = HCatInputFormat.getTableSchema(context.getConfiguration());
			// Read the user field.
			age = value.getInteger("age", schema);
			context.write(new IntWritable(age), new IntWritable(1));
		}
	}

	@SuppressWarnings("rawtypes")
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				sum++;
				iter.next().get();
			}
			HCatRecord record = new DefaultHCatRecord(2);
			record.set(0, key.get());
			record.set(1, sum);
			context.write(null, record);
		}
	}

	public int run(String[] arg) throws Exception {

		// 获得传入参数值
		String dbName = arg[2];
		String inputTableName = arg[3];
		String outputTableName = arg[4];

		System.out.println("===database====" + dbName);
		System.out.println("===inputTableName====" + inputTableName);
		System.out.println("===outputTableName====" + outputTableName);

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("HCatMRTest");
		// Read the inputTableName table, partition null, initialize the default
		// database.
		HCatInputFormat.setInput(job, dbName, inputTableName);

		// Initialize HCatoutputFormat
		job.setInputFormatClass(HCatInputFormat.class);
		job.setJarByClass(HCatMRTest.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);

		String inputJobString = job.getConfiguration().get(
				HCatConstants.HCAT_KEY_JOB_INFO);
		job.getConfiguration().set(HCatConstants.HCAT_KEY_JOB_INFO,
				inputJobString);

		// Write into outputTableName table, partition null, initialize the
		// default database.
		OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName,
				outputTableName, null);
		job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO,
				HCatUtil.serialize(outputJobInfo));
		HCatOutputFormat.setOutput(job, outputJobInfo);

		HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
		HCatOutputFormat.setSchema(job, s);
		job.setOutputFormatClass(HCatOutputFormat.class);
		System.out
				.println("INFO: Output scheme explicity set for writing:" + s);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

		int exitcode = ToolRunner.run(new HCatMRTest(), args);
		System.exit(exitcode);
	}
}
