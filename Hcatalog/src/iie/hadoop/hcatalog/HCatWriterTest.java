package iie.hadoop.hcatalog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

/**
 * 利用HCatWriter接口，读本地文件中的数据，写到hive表里
 * 
 * CREATE TABLE student (name STRING，age INT) ROW FORMAT DELIMITED FIELDS
 * TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
 * 
 * 执行命令：hadoop jar HCatWriterTest.jar config.properties
 * 
 * @author xiaodongfang
 */
public class HCatWriterTest {

	public static void main(String[] args) throws HCatException {
		
		if (args.length < 1) {
			System.err.println("Usage: HCatWriterTest <configuration>");
			System.exit(1);
		}
		String dbName = null;
		String writerTabName = null;
		String writerInFile = null;
		try {
			// 获取配置文件参数
			String confFile = args[0];
			File configFile = new File(confFile);
			// System.out.println("configuration file is:" + confFile);
			InputStream in = new FileInputStream(configFile);
			Properties props = new Properties();
			props.load(in);
			dbName = props.getProperty("dbName");
			writerTabName = props.getProperty("writerTabName");
			writerInFile = props.getProperty("writerInFile");
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase(dbName)
				.withTable(writerTabName).build();
		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		WriterContext context = writer.prepareWrite();
		HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);
		List<HCatRecord> records = new ArrayList<HCatRecord>();

		File writeFile = new File(writerInFile);
		BufferedReader buffer = null;
		String line = null;
		String[] con = null;

		try {
			buffer = new BufferedReader(new InputStreamReader(
					new FileInputStream(writeFile), "UTF-8"));
			line = buffer.readLine();
			while (line != null) {
				List<Object> list = new ArrayList<Object>(2);
				con = line.split("\t", -1);
				list.add(con[0]);
				list.add(Integer.parseInt(con[1]));
				records.add(new DefaultHCatRecord(list));
				line = buffer.readLine();
			}
			buffer.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}

		splitWriter.write(records.iterator());
		writer.commit(context);
		System.out.println("配置文件的位置为：" + args[0]);
		System.exit(0);
	}
}
