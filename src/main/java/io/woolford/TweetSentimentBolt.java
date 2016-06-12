package io.woolford;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class TweetSentimentBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(TweetSentimentBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");

        if (tweet.getLang().equals("en")){

            if (tweet.getText().toLowerCase().contains("trump") | tweet.getText().toLowerCase().contains("hilary") | tweet.getText().toLowerCase().contains("clinton")){
                String tweetText = tweet.getText();
                Properties props = new Properties();
                props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
                StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

                String sentiment = "";
                Annotation annotation = pipeline.process(tweetText);
                List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
                for (CoreMap sentence : sentences) {
                    sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
                }

                logger.info(sentiment + ": " + tweet.toString());
                collector.emit(new Values(tweet.getId(), sentiment));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "sentiment"));
    }

}
