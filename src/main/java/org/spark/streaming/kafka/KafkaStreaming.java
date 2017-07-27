package org.spark.streaming.kafka;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaStreaming {

	public static JavaRDD<String> baseRDD;
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setAppName("Kafka-Spark-Example").setMaster("yarn-client");
		String brokersArgument = args[0];
		String topicsArgument = args[1];
		String inputFileArgument = args[2];

		JavaSparkContext jsc = new JavaSparkContext(new SparkConf());

		JavaStreamingContext ssc = new JavaStreamingContext(jsc, new Duration(5000));

		Map<String, String> params = Maps.newHashMap();
		params.put("metadata.broker.list", brokersArgument);
		Set<String> topics = Sets.newHashSet(topicsArgument);

		baseRDD = jsc.textFile(inputFileArgument);
		System.out.println("File Count ----------------> " + baseRDD.count());

		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(ssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, params, topics);
		
		lines.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {

			@Override
			public void call(JavaPairRDD<String, String> t) throws Exception {
				
				baseRDD = t.values().union(baseRDD);
				System.out.println("File Count after union is ------------> " + baseRDD.count());
			}
			
		});
		
	ssc.start();
	ssc.awaitTermination();
	}

}
