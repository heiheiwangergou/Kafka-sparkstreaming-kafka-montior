package com.anto.kafka.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.Builder;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
 
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		Builder<String, String> builder = KafkaSpoutConfig.builder("yun01:9092", "test");
		builder.setGroupId("g1");
		KafkaSpoutConfig<String, String> build = builder.build();
		
		KafkaSpout<String,String> kafkaSpout = new KafkaSpout<>(build);
		topologyBuilder.setSpout("kafkaSpout", kafkaSpout,1);
		
		topologyBuilder.setBolt("printBot", new PrintBot(),1).localOrShuffleGrouping("kafkaSpout");
		
		LocalCluster cluster = new LocalCluster();
		
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		StormTopology createTopology = topologyBuilder.createTopology();
		cluster.submitTopology("kafkaSpout", config, createTopology);
		
		
		
		
		
		
		
		
		
		
			
		
		
		
		
		
	}
	
	
}
