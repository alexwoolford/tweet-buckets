package io.woolford;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.util.Map;


public class ClintonTrumpFilterBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ClintonTrumpFilterBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        Status tweet = (Status) input.getValueByField("tweet");
        Integer sentiment = (Integer) input.getValueByField("sentiment");

        if (tweet.getText().toLowerCase().contains("trump") || tweet.getText().toLowerCase().contains("clinton")){

            logger.info("sentiment: " + sentiment + "; tweet text: " + tweet.getText());
            //TODO: convert tweet to JSON
            collector.emit(new Values(tweet, sentiment));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "sentiment"));
    }

}
