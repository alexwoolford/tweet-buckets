package io.woolford;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {

    static final String TOPOLOGY_NAME = "tweet-buckets";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("EnglishTweetSentimentBolt", new EnglishTweetSentimentBolt()).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("ClintonTrumpFilterBolt", new ClintonTrumpFilterBolt()).shuffleGrouping("EnglishTweetSentimentBolt");


        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });

    }

}

