package me.sachingupta.assessments;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class Assessment1 {
	
	private SparkSession sc;
	private static Logger log = LogManager.getLogger("Assessment1");
	
	private final String basePath = "src/main/resources/";
	
	public Assessment1() {
		super();
		sc = SparkSession.builder().appName("Assessment 1").master("local").getOrCreate();
		log.info("Spark session created.....");
	}

	public void execute() {
		Dataset<Row> data = prepareAndGetAnalyticalData();
		Dataset<Row> states = prepareAndGetStateData();
		
		// Dataset<Row> joined_data = data.join(states);
		
		data.show();
		states.show();
		
		//joined_data.show();
	}
	
	private Dataset<Row> prepareAndGetStateData() {
		
		
		return sc.read().option("header", true).csv(basePath+"assessment1/indian_states_code_name_mapping.csv");
	}
	
	private Dataset<Row> prepareAndGetAnalyticalData() {
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode rootNode = null;
		try {
			JsonNode states = mapper.readTree(new File(basePath + "assessment1/covid.json"));
			rootNode = mapper.createArrayNode();
			
			Iterator<String> itr = states.fieldNames();
		
			
			while(itr.hasNext()) {
				String nodeName = itr.next();
				JsonNode node = states.get(nodeName);
				if(node.get("districts") == null)
					continue;
				else {
					Iterator<String> itr2 = node.get("districts").fieldNames();
					while(itr2.hasNext()) {
						String district = itr2.next();
						JsonNode districtNode = node.get(district);
						
						ObjectNode newNode = mapper.createObjectNode();
						newNode.put("state", nodeName);
						newNode.put("district", district);
						
						System.out.println(districtNode.asText());
						if(districtNode.get("total").get("confirmed") != null) {
							newNode.put("confirmed", districtNode.get("total").get("confirmed").asLong());
						}
						if(districtNode.get("total").get("deceased") != null) {
							newNode.put("deceased", districtNode.get("total").get("deceased").asLong());
						}
						rootNode.add(newNode);
						
					}
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sc.read().json(rootNode.toString());
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null)
			sc.close();
		log.info("Spark session closed....");
	}

	
}
