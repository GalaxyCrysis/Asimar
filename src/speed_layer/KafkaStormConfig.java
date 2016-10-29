package speed_layer;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * Created by Dominik on 29.10.2016.
 */
public class KafkaStormConfig {
    public static void main(String[] args) throws Exception{
        String topic = "Asimar";
        //config for topology
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING,1);

        //zookeeper host
        String zookeeper= "localhost:2181";
        //init kafka brokers
        BrokerHosts broker = new ZkHosts(zookeeper);

        //init spout and set configs
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(broker,topic,"/"+topic, UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024*1024*4;
        kafkaSpoutConfig.fetchSizeBytes = 1024*1024*4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //create storm toplogy
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout",new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("split-bolt",new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("process-bolt",new processBolt()).shuffleGrouping("split-bolt");
        builder.setBolt("cassandra-bolt",new CassandraBolt()).shuffleGrouping("process-bolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Asimar",config,builder.createTopology());
        Thread.sleep(20000);
        cluster.shutdown();


    }

}
