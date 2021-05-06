package me.sachingupta.assessments;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple4;

public class Assessment3 implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private JavaSparkContext sc;
	
	private static Logger log = LogManager.getLogger("Assessment1");
	
	private final String basePath = "src/main/resources/";

	public Assessment3() {
		super();
		SparkConf config = new SparkConf().setAppName("Assessment 3").setMaster("local");
		sc = new JavaSparkContext(config);
	}

	public void execute() {
		JavaRDD<String> rawData = sc.textFile(basePath + "assessment3/Pet.csv");
		rawData = rawData.filter(s -> !s.startsWith("id,"));
		JavaRDD<Tuple4<Long, Long, String, String>> data = rawData.map(d -> {
			String[] str = d.split(",");
			
			return new Tuple4<Long, Long, String, String>(Long.parseLong(str[0]), Long.parseLong(str[1]), str[2], str[3]);
		});
		
		JavaPairRDD<String, Tuple2<Long, String>> data1 = data.mapToPair(d -> new Tuple2<String, Tuple2<Long, String>>(d._3(), new Tuple2<Long, String>(d._2(), d._4())) );
		
		// data1.collect().forEach(System.out::println);
		// data1.groupByKey().collect().forEach(System.out::println);
		JavaPairRDD<Long, Long> data2 = data1.groupByKey().flatMapToPair(d -> {
			List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
			// System.out.println("breed" + d._1);
			Iterator<Tuple2<Long, String>> itr = d._2().iterator();
			Iterator<Tuple2<Long, String>> itr1 = d._2().iterator();
			
			Tuple2<Long, String> t1;
			Tuple2<Long, String> t2;
			
			/*t1 = itr.next();
			while(itr.hasNext()) {
				t2 = itr.next();
				if(t1._1() != t2._1()) {
					
					if(t1._2().equalsIgnoreCase("male") && t2._2().equalsIgnoreCase("female")) {
						list.add(new Tuple2<>(t1._1(), new Long(1)));
					} else if(t1._2().equalsIgnoreCase("female") && t2._2().equalsIgnoreCase("male")) {
						list.add(new Tuple2<>(t1._1(), new Long(1)));
					} else {
						list.add(new Tuple2<>(t1._1(), new Long(0)));
					}
					
				} else {
					list.add(new Tuple2<>(t1._1(), new Long(0)));
				}
				t1 = t2;
			}*/
			
			while(itr.hasNext()) {
				t1 = itr.next();
				itr1 = d._2().iterator();
				while(itr1.hasNext()) {
					t2 = itr1.next();
					
					if(t1._1() != t2._1()) {
						
						if(t1._2().equalsIgnoreCase("male") && t2._2().equalsIgnoreCase("female")) {
							list.add(new Tuple2<>(t1._1(), new Long(1)));
						} else if(t1._2().equalsIgnoreCase("female") && t2._2().equalsIgnoreCase("male")) {
							list.add(new Tuple2<>(t1._1(), new Long(1)));
						} else {
							list.add(new Tuple2<>(t1._1(), new Long(0)));
						}
						
					} else {
						list.add(new Tuple2<>(t1._1(), new Long(0)));
					}
				}
			}
			
			
			return list.iterator();
		});
		// data2.collect().forEach(System.out::println);
		data2 = data2.reduceByKey((r1, r2) -> r1 + r2);
		
		/*Long parts = data2.values().reduce((a, b) -> Math.max(a, b));
		
		JavaPairRDD<Tuple2<Long, Long>, Long> data3 = data2.mapToPair(d -> new Tuple2<Tuple2<Long, Long>, Long>(d, d._2()))
				.sortByKey(new TupleValueComparator(), false, parts.intValue());
		data2 = data3.mapToPair(d -> new Tuple2<Long, Long>(d._1._1, d._1._2));
		*/System.out.println("owner_id,count");
		data2.collect().forEach(t -> System.out.println(t._1() + "," + t._2()));
	}
	
	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		super.finalize();
		if(sc != null)
			sc.close();
		log.info("Spark session closed....");
	}
	
	class TupleValueComparator implements Comparator<Tuple2<Long, Long>>,Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Tuple2<Long, Long> tuple1, Tuple2<Long, Long> tuple2) {
			if (tuple1._2.compareTo(tuple2._2) == 0) {
	            return tuple1._1.compareTo(tuple2._1);
	        }
	        return -tuple1._2.compareTo(tuple2._2);
		}
	}
	
}


