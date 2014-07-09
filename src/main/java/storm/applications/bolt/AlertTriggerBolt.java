package storm.applications.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;
import storm.applications.util.BFPRT;

/**
 * The AlertTriggerBolt triggers an alert if a stream is identified as abnormal.
 * @author yexijiang
 *
 */
public class AlertTriggerBolt extends BaseRichBolt {
    private static final double dupper = Math.sqrt(2);
    private long previousTimestamp;
    private OutputCollector collector;
    private List<Tuple> streamList;
    private double minDataInstanceScore = Double.MAX_VALUE;
    private double maxDataInstanceScore = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.previousTimestamp = 0;
        this.streamList = new ArrayList<Tuple>();
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLong(2);
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
                    if((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        // check whether cur data instance score return to normal
                        if(curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }

                    //if 
                    collector.emit(new Values(streamProfile.getString(0), streamScore, streamProfile.getLong(2), isAbnormal, streamProfile.getValue(3)));
                }
                
                this.streamList.clear();
                minDataInstanceScore = Double.MAX_VALUE;
                maxDataInstanceScore = 0;
            }

            this.previousTimestamp = timestamp;
        }

        double dataInstScore = input.getDouble(4);
        if (dataInstScore > maxDataInstanceScore) {
            maxDataInstanceScore = dataInstScore;
        }
        
        if (dataInstScore < minDataInstanceScore) {
            minDataInstanceScore = dataInstScore;
        }

        this.streamList.add(input);
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ANOMALY_STREAM_FIELD, STREAM_ANOMALY_SCORE_FIELD, 
                TIMESTAMP_FIELD, IS_ABNORMAL_FIELD, OBSERVATION_FIELD));
    }

    /**
     * Identify the abnormal streams.
     * @return
     */
    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<Tuple>();
        int medianIdx = (int)(streamList.size() / 2);
        BFPRT.bfprt(streamList, medianIdx);
        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}