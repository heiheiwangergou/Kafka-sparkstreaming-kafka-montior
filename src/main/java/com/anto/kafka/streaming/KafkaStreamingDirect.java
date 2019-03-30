//package com.anto.kafka.streaming;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import kafka.serializer.StringDecoder;
//import scala.Tuple2;
//
//public class KafkaStreamingDirect {
//
//	public static void main(String[] args) {
//	 
//		SparkConf conf = new SparkConf().setMaster("local").setAppName("direct");
//	
//		JavaStreamingContext jsc = new JavaStreamingContext(conf,Durations.seconds(5));
//		Map<String, String> paramMap = new HashMap<>();
//		paramMap.put("metadata.broker.list", "yun01:9092");
//		Set<String> topic = new HashSet<>();
////		topic.add("test2");
//		JavaPairInputDStream<String, String> ds = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, 
//				paramMap , topic);
//		
//		JavaDStream<String> words = ds.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
//			private static final long serialVersionUID = 1L;
//
//			public java.lang.Iterable<String> call(Tuple2<String,String> t) throws Exception {
//				
//				String[] splited = t._2.split(" ");
//				
//				return Arrays.asList(splited);
//				
//			};
//			
//		});
//		
//		JavaPairDStream<String, Long> wordCount  = words.mapToPair(new PairFunction<String, String, Long>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Long> call(String t) throws Exception {
//				
//				return new Tuple2<String, Long>(t, 1L);
//			}
//		}).reduceByKey(new Function2<Long, Long, Long>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Long call(Long v1, Long v2) throws Exception {
//			
//				return v1+v2;
//			}
//		}).map(new Function<Tuple2<String,Long>,Tuple2<String,Long>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Long> call(Tuple2<String, Long> v1) throws Exception {
//				
//				return v1;
//			}
//		}).mapToPair(new PairFunction<Tuple2<String,Long>, String, Long>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
//				return  t;
//			}
//		});
//		wordCount.print();
//		
//		jsc.start();
//		jsc.awaitTermination();
//		jsc.close();
//	}
//	
//	
//	
//	
//}
