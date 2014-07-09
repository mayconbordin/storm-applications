package storm.applications.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;

/**
 * Summing up all the data instance scores as the stream anomaly score.
 * @author yexijiang
 *
 */
public class SlidingWindowStreamAnomalyScoreBolt extends BaseRichBolt {
    // hold the recent scores for each stream
    private Map<String, Queue<Double>> slidingWindowMap;
    private int windowLength;
    private OutputCollector collector;
    private long previousTimestamp;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Object objL = stormConf.get("sliding.window.length");
        if (objL == null || objL.toString().trim().length() == 0) {
            this.windowLength = 10;
        } else {
            this.windowLength = Integer.parseInt(objL.toString());
        }
        this.slidingWindowMap = new HashMap<String, Queue<Double>>();
        this.collector = collector;
        this.previousTimestamp = 0;
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLong(2);
        String id = input.getString(0);
        double dataInstanceAnomalyScore = input.getDouble(1);
        Queue<Double> slidingWindow = slidingWindowMap.get(id);
        if (slidingWindow == null) {
            slidingWindow = new LinkedList<Double>();
        }

        // update sliding window
        slidingWindow.add(dataInstanceAnomalyScore);
        if (slidingWindow.size() > this.windowLength) {
                slidingWindow.poll();
        }
        slidingWindowMap.put(id, slidingWindow);

        double sumScore = 0.0;
        for (double score : slidingWindow) {
            sumScore += score;
        }
        
        this.collector.emit(new Values(id, sumScore, timestamp, input.getValue(3), dataInstanceAnomalyScore));
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID_FIELD, STREAM_ANOMALY_SCORE_FIELD, TIMESTAMP_FIELD,
            OBSERVATION_FIELD, CUR_DATAINST_SCORE_FIELD));
    }
}
