package com.harman.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
//import com.mongodb.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
//import org.scalatest.tools.Durations;

import com.mongodb.spark.config.WriteConfig;

public class SparkClient {

	public static void printRDD(JavaRDD<String> s) {
		s.foreach(System.out::println);
	}

	/*
	 * public static void inserIntoMongoDB(String str){
	 * 
	 * SparkSession spark = SparkSession.builder().master("local")
	 * .appName("SmartAudioAnalytics") .config("spark.mongodb.input.uri",
	 * "mongodb://127.0.0.1/DEVICE_INFO_STORE.SmartAudioAnalytics")
	 * .config("spark.mongodb.output.uri",
	 * "mongodb://127.0.0.1/DEVICE_INFO_STORE.SmartAudioAnalytics")
	 * .getOrCreate(); JavaSparkContext jsc = new
	 * JavaSparkContext(spark.sparkContext()); // Create a custom WriteConfig
	 * Map<String, String> writeOverrides = new HashMap<String, String>();
	 * writeOverrides.put("collection", "SmartAudioAnalytics");
	 * writeOverrides.put("writeConcern.w", "majority"); WriteConfig writeConfig
	 * = WriteConfig.create(jsc).withOptions(writeOverrides);
	 * MongoSpark.save(jRDD, writeConfig); }
	 */
	public static void main(String[] args) {

		System.out.println("In main spark Client");
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SmartAudioAnalytics");
		// sparkConf.setMaster("local");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

		JavaReceiverInputDStream<String> JsonReq = ssc.socketTextStream("localhost", 9997,
				StorageLevels.MEMORY_AND_DISK_SER);

		JsonReq.foreachRDD(SparkClient::printRDD);

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		// line.print();
		System.out.println("JRI");
		DStream<String> dstream = JsonReq.dstream();

		/*SparkSession spark = SparkSession.builder().master("local").appName("SmartAudioAnalytics")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.getOrCreate();*/
		
		SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
			      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
			      .getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		/*// Create a custom WriteConfig
		Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("collection", "SmartAudioAnalytics");
		writeOverrides.put("writeConcern.w", "majority");
		WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);*/
		
		// Create a custom WriteConfig
	    Map<String, String> writeOverrides = new HashMap<String, String>();
	    writeOverrides.put("collection", "spark");
	    writeOverrides.put("writeConcern.w", "majority");
	    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
		
		
		System.out.println("start making RDD");
		List<Document> list = new ArrayList<>();

		list.add(Document.parse(dstream.toString()));
		// JsonObject jsonObject=new JsonObject();
		// jsonObject.addProperty("macaddress", "sddjkdjdjdj");
		// list.add(Document.parse(jsonObject.toString()));
		// Document doc = Document.parse("{Test JSON:1}");
		JavaRDD<Document> jRDD = jsc.parallelize(list);
		System.out.println("RDD is ready");
		// jRDD.w
		System.out.println("Before mongoSpark");
		MongoSpark.save(jRDD, writeConfig);

		/*
		 * MongoSpa
		 * 
		 * JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 * 
		 * JavaRDD<Document> documents = jsc.parallelize
		 * 
		 * ssc.
		 * 
		 * val writeConfig = WriteConfig(Map("collection" ->
		 * "SmartAudioAnalytics", "writeConcern.w" -> "majority"),
		 * Some(WriteConfig(ssc))); val sparkDocuments = ssc.parallelize((1 to
		 * 2).map(i => Document.parse(s"{spark: $line}")));
		 * 
		 * MongoSpark.save(sparkDocuments, writeConfig) SQLContext sqlContext =
		 * spark.sqlContext();
		 * 
		 * json.foreachRDD(new VoidFunction<JavaRDD<String>>() {
		 * 
		 * @Override public void call(JavaRDD<String> rdd) { if(!rdd.isEmpty()){
		 * Dataset<Row> data = spark.read().json(rdd).select("harmanDevice");
		 * data.printSchema(); data.show(false); //DF in table Dataset<Row> df =
		 * data.select(
		 * org.apache.spark.sql.functions.explode(org.apache.spark.sql.functions
		 * .col("sensors"))).toDF("sensors").select("sensors.s","sensors.d");
		 * df.show(false); } } });
		 * 
		 */ ssc.start();

		ssc.awaitTermination();
		// ssc.stop();

	}

}