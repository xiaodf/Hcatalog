package iie.hadoop.hcatalog.spark;


import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.scala.SerHCatOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.Accumulator;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
//import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.thrift.TException;

import scala.Tuple2;

/**
 * spark+hcatalog操作hive表，将表中数据复制到另一张相同结构的表中 create table test(col1 String,col2
 * int);
 * 
 * @author xiaodongfang
 *
 */
public class LowerUpperCaseConvert {

	private static Accumulator<Integer> inputDataCount;
	private static Accumulator<Integer> outputDataCount;

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}

		String inputXml = args[1];
		String userName = null;
		String jobinstanceid = null;
		String operatorName = null;
		String dbName = null;
		String inputTabName = null;
		String operFieldName = null;
		int fieldCount = 0;

		// 读取xml文件
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dis = fs.open(new Path(inputXml));
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader read = new BufferedReader(isr);
		String tempString = "";
		String xmlParams = "";
		while ((tempString = read.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		read.close();
		xmlParams = xmlParams.substring(1);

		// 获取xml文件中的参数值
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(xmlParams);
		userName = list.get(0).get("userName").toString();
		dbName = list.get(0).get("dbName").toString();
		inputTabName = list.get(0).get("inputTabName").toString();
		operatorName = list.get(0).get("operatorName").toString();
		jobinstanceid = list.get(0).get("jobinstanceid").toString();
		fieldCount = Integer.parseInt(list.get(0).get("fieldCount").toString());

		// 设置输出表字段名及类型
		ArrayList<String> fieldName = new ArrayList<String>();
		ArrayList<String> fieldType = new ArrayList<String>();
		for (int i = 1; i <= fieldCount; i++) {
			fieldName.add(list.get(0).get("fieldName" + i).toString());
			fieldType.add(list.get(0).get("fieldType" + i).toString());
		}
		String[] fieldNames = new String[fieldCount];
		String[] fieldTypes = new String[fieldCount];

		// 设置输出表的名字
		String[] outputStr = UUID.randomUUID().toString().split("-");
		String outputTable = "tmp";
		for (int i = 0; i < outputStr.length; i++) {
			outputTable = outputTable + "_" + outputStr[i];
		}
		
		// 获取表字段名字和类型
		for (int j = 0; j < fieldCount; j++) {
			fieldNames[j] = fieldName.get(j);
			fieldTypes[j] = fieldType.get(j);
			System.out.println("====fieldName=====" + fieldNames[j]);
			System.out.println("====fieldType=====" + fieldTypes[j]);
		}
		System.out.println("====fieldCount=====" + fieldCount);
		// 创建hive表
		HCatSchema schema = getHCatSchema(dbName, inputTabName);
		createTable(dbName, outputTable, schema);

		// 将输入表字段数据转换为大写，写入输出表文件中
		JavaSparkContext jsc = new JavaSparkContext(
				new SparkConf().setAppName("LowerUpperCaseConvert"));
		inputDataCount = jsc.accumulator(0);
		outputDataCount = jsc.accumulator(0);

		// 要操作的字段名称及字段序号
		operFieldName = fieldNames[0];
		System.out.println("====operFieldName======"+operFieldName);
		int position = schema.getPosition(operFieldName);

		JavaRDD<SerializableWritable<HCatRecord>> rdd1 = LowerUpperCaseConvert
				.lowerUpperCaseConvert(jsc, dbName, inputTabName, position);
		LowerUpperCaseConvert.storeToTable(rdd1, dbName, outputTable);
		jsc.stop();

		// 设置输出xml文件参数
		List<Map> listOut = new ArrayList<Map>();
		Map<String, String> mapOut = new HashMap<String, String>();
		mapOut.put("jobinstanceid", jobinstanceid);
		mapOut.put("dbName", dbName);
		mapOut.put("outputTable", outputTable);
		mapOut.put("inputDataCount", inputDataCount.value().toString());
		mapOut.put("outputDataCount", outputDataCount.value().toString());
		listOut.add(mapOut);

		// 创建正常输出xml文件
		String hdfsOutXml = "/user/" + userName + "/optasks/" + jobinstanceid
				+ "/" + operatorName + "/out" + "/stdout.xml";
		operXML.genStdoutXml(hdfsOutXml, listOut);

		// 创建错误输出xml文件
		String operFieldType = fieldTypes[0];
		if (!(operFieldType.equalsIgnoreCase("String"))) {
			String hdfsErrorXml = "/user/" + userName + "/optasks/"
					+ jobinstanceid + "/" + operatorName + "/out"
					+ "/stderr.xml";
			operXML.genStderrXml(hdfsErrorXml, listOut);
		}
		System.exit(0);
	}

	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> lowerUpperCaseConvert(
			JavaSparkContext jsc, String dbName, String inputTabName,
			int position) throws IOException {

		Configuration inputConf = new Configuration();
		SerHCatInputFormat.setInput(inputConf, dbName, inputTabName);

		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(inputConf, SerHCatInputFormat.class,
						WritableComparable.class, SerializableWritable.class);
		
		final Broadcast<Integer> posBc = jsc.broadcast(position);
		// 获取表记录集
		JavaRDD<SerializableWritable<HCatRecord>> result = null;
		final Accumulator<Integer> output = jsc.accumulator(0);
		final Accumulator<Integer> input = jsc.accumulator(0);

		result = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {

					private static final long serialVersionUID = -2362812254158054659L;

					private final int postion = posBc.getValue().intValue();

					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2.value();
						// +1 inport
						input.add(1);
						List<Object> newRecord = new ArrayList<Object>(record
								.size());
						for (int i = 0; i < record.size(); ++i) {
							newRecord.add(record.get(i));
						}
						/*
						 * if (ok) +1 outport1 else +1 errport
						 */
						newRecord.set(postion, newRecord.get(postion)
								.toString().toUpperCase());
						output.add(1);
						return new SerializableWritable<HCatRecord>(
								new DefaultHCatRecord(newRecord));// 返回记录
					}
				});
		inputDataCount = input;
		outputDataCount = output;
		return result;
	}

	@SuppressWarnings("rawtypes")
	public static void storeToTable(
			JavaRDD<SerializableWritable<HCatRecord>> rdd, String dbName,
			String tblName) {
		Job outputJob = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("lowerUpperCaseConvert");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			SerHCatOutputFormat.setSchema(outputJob, schema);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 将RDD存储到目标表中
		rdd.mapToPair(
				new PairFunction<SerializableWritable<HCatRecord>, WritableComparable, SerializableWritable<HCatRecord>>() {

					private static final long serialVersionUID = -4658431554556766962L;

					@Override
					public Tuple2<WritableComparable, SerializableWritable<HCatRecord>> call(
							SerializableWritable<HCatRecord> record)
							throws Exception {
						return new Tuple2<WritableComparable, SerializableWritable<HCatRecord>>(
								NullWritable.get(), record);
					}
				}).saveAsNewAPIHadoopDataset(outputJob.getConfiguration());
	}

	/* 创建表结构 */
	public static void createTable(String dbName, String tblName,
			HCatSchema schema) {
		HiveMetaStoreClient client = null;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			client = HCatUtil.getHiveClient(hiveConf);
		} catch (MetaException | IOException e) {
			e.printStackTrace();
		}
		try {
			if (client.tableExists(dbName, tblName)) {
				client.dropTable(dbName, tblName);
			}
		} catch (TException e) {
			e.printStackTrace();
		}

		List<FieldSchema> fields = HCatUtil.getFieldSchemaList(schema
				.getFields());
		System.out.println(fields);
		Table table = new Table();
		table.setDbName(dbName);
		table.setTableName(tblName);

		StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(fields);
		table.setSd(sd);
		sd.setInputFormat(RCFileInputFormat.class.getName());
		sd.setOutputFormat(RCFileOutputFormat.class.getName());
		sd.setParameters(new HashMap<String, String>());
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setName(table.getTableName());
		sd.getSerdeInfo().setParameters(new HashMap<String, String>());
		sd.getSerdeInfo().getParameters()
				.put(serdeConstants.SERIALIZATION_FORMAT, "1");
		sd.getSerdeInfo().setSerializationLib(
				org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
						.getName());
		Map<String, String> tableParams = new HashMap<String, String>();
		table.setParameters(tableParams);
		try {
			client.createTable(table);
			System.out.println("Create table successfully!");
		} catch (TException e) {
			e.printStackTrace();
			return;
		} finally {
			client.close();
		}
	}

//	// 获得HCatSchema
//	@SuppressWarnings("deprecation")
//	public static HCatSchema getHCatSchema(String[] fieldNames,
//			String[] fieldTypes) throws HCatException {
//		List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(
//				fieldNames.length);
//		// PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
//		for (int i = 0; i < fieldNames.length; ++i) {
//			HCatFieldSchema.Type type = HCatFieldSchema.Type
//					.valueOf(fieldTypes[i].toUpperCase());
//			fieldSchemas.add(new HCatFieldSchema(fieldNames[i], type, ""));
//		}
//		return new HCatSchema(fieldSchemas);
//	}

	// 获得HCatSchema
	public static HCatSchema getHCatSchema(String dbName, String tblName) {
		Job outputJob = null;
		HCatSchema schema = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("getHCatSchema");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return schema;
	}
}
