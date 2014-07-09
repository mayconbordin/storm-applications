package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.applications.bolt.CalculateSentimentBolt;
import storm.applications.bolt.LocateStateBolt;
import storm.applications.spout.TwitterStreamSpout;
import storm.applications.topology.AbstractTopology;
import twitter4j.FilterQuery;

/**
 * Orchestrates the elements and forms a Topology to find the most happiest state
 * by analyzing and processing Tweets.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class SentimentAnalysisTopology extends AbstractTopology {

    public SentimentAnalysisTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void prepare() {
        
    }
    
    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        
        // TODO: http://altf1be.wordpress.com/2013/06/14/designing-a-solution-using-real-time-stock-quotes-analytics-with-storm-twitter-yahoo-finance-bigdata-sentimentanalysis-ghent-belgium/
        // tweets from stock quotes
        
        // implement the sentiment analysis as an engine
        // so that we can have multiple engines
        
        //Bounding Box for United States.
        //For more info on how to get these coordinates, check:
        //http://www.quora.com/Geography/What-is-the-longitude-and-latitude-of-a-bounding-box-around-the-continental-United-States
        double[][] boundingBox = {{-124.848974, 24.396308}, {-66.885444, 49.384358}};
        
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.locations(boundingBox);
        
        builder.setSpout("twitterspout", new TwitterStreamSpout(filterQuery));
        
        builder.setBolt("statelocatorbolt", new LocateStateBolt())
               .shuffleGrouping("twitterspout");
        
        //Create Bolt with the frequency of logging [in seconds].
        builder.setBolt("sentimentcalculatorbolt", new CalculateSentimentBolt(30))
               .fieldsGrouping("statelocatorbolt", new Fields("state"));
        
        return builder.createTopology();
    }
    
}
