package me.sachingupta.assessments;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.exp;
import static org.apache.spark.sql.functions.lit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

public class Assessment2 {

	private SparkSession sc;
	private static Logger log = LogManager.getLogger("Assessment1");
	
	private final String basePath = "src/main/resources/";
	
	public Assessment2() {
		super();
		sc = SparkSession.builder().appName("Assessment 1").master("local").getOrCreate();
		log.info("Spark session created.....");
		sc.udf().register("calculateSlope", calculateSlope, DataTypes.DoubleType);
	}

	public void execute() {
		Dataset<Row> data = sc.read()
				.option("header", true)
				.csv(basePath + "assessment2/segments.csv");
		data = data.withColumn("slope", callUDF("calculateSlope", col("x1").cast(DataTypes.DoubleType), col("x2").cast(DataTypes.DoubleType), col("y1").cast(DataTypes.DoubleType), col("y2").cast(DataTypes.DoubleType)));
		data = data.withColumn("count", (count("slope").over(Window.partitionBy("slope"))).minus(new Integer(1)) );
		
		Dataset<Row> data_out = data.select(col("id"), col("count")).orderBy(col("count").desc(), col("id").cast(DataTypes.LongType).asc());
		
		data_out.show();
		
		System.out.println("id,count");
		data_out.javaRDD().collect().forEach(t -> System.out.println(t.get(0) + "," + t.get(1)));
		
	}
	
	private  static UDF4<Double, Double, Double, Double, Double> calculateSlope = new UDF4<Double, Double, Double, Double, Double>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Double call(Double x1, Double x2, Double y1, Double y2) throws Exception {
		    double denominator = (x2 - x1);
		    if(denominator == 0)
		        return (double) 0.0f;
			return (y2 - y1)/ denominator;
		}
		
	};

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null)
			sc.close();
		log.info("Spark session closed....");
	}

}
