package com.hwxlabs.storm.StreamKafka2Hdfs;

import com.google.common.io.Resources;
import com.hwxlabs.storm.StreamKafka2Hdfs.bolt.HdfsBoltBuilder;
import com.hwxlabs.storm.StreamKafka2Hdfs.spout.KafkaSpoutBuilder;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.kafka.KafkaSpout;

import java.util.Properties;


public class Topology {
	
	public Properties configs;
	public HdfsBoltBuilder boltBuilder;
	public KafkaSpoutBuilder spoutBuilder;
	public static final String HDFS_STREAM = "hdfs-stream";
	

	public Topology(String configFile) throws Exception {
		configs = new Properties();
		try {
			// to change to configFile ...
			configs.load(Topology.class.getResourceAsStream("/default_config.properties"));
			boltBuilder = new HdfsBoltBuilder(configs);
			spoutBuilder = new KafkaSpoutBuilder(configs);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}
	}

	private void submitTopology() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();	
		KafkaSpout kafkaSpout = spoutBuilder.buildKafkaSpout();
		//SinkTypeBolt sinkTypeBolt = boltBuilder.buildSinkTypeBolt();
		HdfsBolt hdfsBolt = boltBuilder.buildHdfsBolt();


		int kafkaSpoutCount = Integer.parseInt(configs.getProperty(Keys.KAFKA_SPOUT_INSTANCE_COUNT));
		builder.setSpout(configs.getProperty(Keys.KAFKA_SPOUT_ID), kafkaSpout, kafkaSpoutCount);

		//int sinkBoltCount = Integer.parseInt(configs.getProperty(Keys.SINK_BOLT_COUNT));
		//builder.setBolt(configs.getProperty(Keys.SINK_TYPE_BOLT_ID),sinkTypeBolt,sinkBoltCount).shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID));

		int hdfsBoltCount = Integer.parseInt(configs.getProperty(Keys.HDFS_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.HDFS_BOLT_ID),hdfsBolt,hdfsBoltCount).shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID));

		Config conf = new Config();
		String topologyName = configs.getProperty(Keys.TOPOLOGY_NAME);

		conf.setNumWorkers(Integer.parseInt(configs.getProperty(Keys.NUM_OF_WORKERS)));
		StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {
		String configFile;
		if (args.length == 0) {
			System.out.println("Missing input : config file location, using default");
			configFile = "default_config.properties";
			
		} else{
			configFile = args[0];
		}
		
		Topology ingestionTopology = new Topology(configFile);
		ingestionTopology.submitTopology();
	}
}
