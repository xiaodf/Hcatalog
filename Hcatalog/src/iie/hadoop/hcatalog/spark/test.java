package iie.hadoop.hcatalog.spark;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class test {

	public static void main(String[] args) throws Exception {

		String stdinXml = "F:/stdin.xml";
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);
		
		String partitionStr = list.get(0).get("sourceTable").toString();
		System.out.println(partitionStr);

	}
}
