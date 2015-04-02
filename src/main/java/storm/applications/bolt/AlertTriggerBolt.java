package storm.applications.bolt;

import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;
import storm.applications.util.math.BFPRT;

/**
 * The AlertTriggerBolt triggers an alert if a stream is identified as abnormal.
 * @author yexijiang
 *
 */
public class AlertTriggerBolt extends AbstractBolt {
    private static final double dupper = Math.sqrt(2);
    private long previousTimestamp;
    private List<Tuple> streamList;
    private double minDataInstanceScore = Double.MAX_VALUE;
    private double maxDataInstanceScore = 0;

    @Override
    public void initialize() {
        previousTimestamp = 0;
        streamList = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(Field.TIMESTAMP);
        
        if (timestamp > previousTimestamp) {
            // new batch of stream scores
            if (!streamList.isEmpty()) {
                List<Tuple> abnormalStreams = this.identifyAbnormalStreams();
                int medianIdx = (int) streamList.size() / 2;
                double minScore = abnormalStreams.get(0).getDouble(1);
                double medianScore = abnormalStreams.get(medianIdx).getDouble(1);
                
                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore = streamProfile.getDouble(1);
                    double curDataInstScore = streamProfile.getDouble(4);
                    boolean isAbnormal = false;

                    // current stream score deviates from the majority
                    if ((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        // check whether cur data instance score return to normal
                        if (curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }

                    if (isAbnormal) { 
                        collector.emit(new Values(streamProfile.getString(0), streamScore, streamProfile.getLong(2), isAbnormal, streamProfile.getValue(3)));
                    }
                }
                
                streamList.clear();
                minDataInstanceScore = Double.MAX_VALUE;
                maxDataInstanceScore = 0;
            }

            previousTimestamp = timestamp;
        }

        double dataInstScore = input.getDouble(4);
        if (dataInstScore > maxDataInstanceScore) {
            maxDataInstanceScore = dataInstScore;
        }
        
        if (dataInstScore < minDataInstanceScore) {
            minDataInstanceScore = dataInstScore;
        }

        streamList.add(input);
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ANOMALY_STREAM, Field.STREAM_ANOMALY_SCORE, Field.TIMESTAMP, Field.IS_ABNORMAL, Field.OBSERVATION);
    }

    /**
     * Identify the abnormal streams.
     * @return
     */
    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<>();
        int medianIdx = (int)(streamList.size() / 2);
        BFPRT.bfprt(streamList, medianIdx);
        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}