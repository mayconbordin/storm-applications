package storm.applications.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;

/**
 * Summing up all the data instance scores as the stream anomaly score.
 * @author yexijiang
 *
 */
public class SlidingWindowStreamAnomalyScoreBolt extends AbstractBolt {
    // hold the recent scores for each stream
    private Map<String, Queue<Double>> slidingWindowMap;
    private int windowLength;
    private long previousTimestamp;

    @Override
    public void initialize() {
        windowLength = config.getInt(Conf.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        slidingWindowMap = new HashMap<>();
        previousTimestamp = 0;
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(Field.TIMESTAMP);
        String id = input.getStringByField(Field.ID);
        double dataInstanceAnomalyScore = input.getDoubleByField(Field.DATAINST_ANOMALY_SCORE);
        
        Queue<Double> slidingWindow = slidingWindowMap.get(id);
        if (slidingWindow == null) {
            slidingWindow = new LinkedList<>();
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
        
        collector.emit(new Values(id, sumScore, timestamp, input.getValue(3), dataInstanceAnomalyScore));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.STREAM_ANOMALY_SCORE, Field.TIMESTAMP, Field.OBSERVATION, Field.CUR_DATAINST_SCORE);
    }
}
