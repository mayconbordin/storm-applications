package storm.applications.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StreamAnomalyScoreBolt extends BaseRichBolt {
    private Map<String, Double> accumulateScores;
    private OutputCollector collector;
    private double lambda;
    private long previousTimestamp;
    private Set<Long> set = new HashSet<Long>();

    private String output = "";

    public StreamAnomalyScoreBolt() {
        this.accumulateScores = new HashMap<String, Double>();
        this.lambda = 0;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.lambda = Double.parseDouble(stormConf.get("lambda").toString());
    }

    @Override
    //	update the stream 
    public void execute(Tuple input) {
        long timestamp = input.getLong(1);
        set.add(timestamp);

        if (previousTimestamp != timestamp) { 
            if (previousTimestamp != 0) {
                System.out.println("\n\n\n\n\n\n" + output + "\n\n\n\n\n\n\n");
                output = "";
            }

            previousTimestamp = timestamp;
        }

        String entityId = input.getString(0);
        double dataInstanceScore = input.getDouble(2);
        Double streamScore = accumulateScores.get(entityId);
        if (streamScore == null) {
            streamScore = 0.0;
        }
        
        streamScore = dataInstanceScore + streamScore * Math.exp(-lambda * (previousTimestamp == 0? 1 : timestamp - previousTimestamp));
        accumulateScores.put(entityId, streamScore);
        output += "EntityID:" + entityId + "\t\tAccummulated Score:" + streamScore + "\n";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}