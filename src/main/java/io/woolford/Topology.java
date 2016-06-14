package io.woolford;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {

    static final String TOPOLOGY_NAME = "tweet-buckets";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        // Use pipe as record boundary
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //Synchronize data buffer with the filesystem every 1000 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(2);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/clinton_trump_tweets");

        // Instantiate the HdfsPersistBolt
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://hadoop01:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);


        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("EnglishTweetSentimentBolt", new EnglishTweetSentimentBolt()).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("ClintonTrumpFilterBolt", new ClintonTrumpFilterBolt()).shuffleGrouping("EnglishTweetSentimentBolt");
        b.setBolt("HdfsBolt", hdfsBolt).shuffleGrouping("ClintonTrumpFilterBolt");


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

