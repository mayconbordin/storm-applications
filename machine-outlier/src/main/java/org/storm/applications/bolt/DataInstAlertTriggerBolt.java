package org.storm.applications.bolt;

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
import static org.storm.applications.MachineOutlierConstants.*;
import org.storm.applications.util.BFPRT;

/**
 * The alert is triggered solely by the anomaly score of current data instance.
 * @author Yexi Jiang (http://users.cs.fiu.edu/~yjian004)
 *
 */
public class DataInstAlertTriggerBolt extends BaseRichBolt {
    private static final double dupper = 1.0;
    private long previousTimestamp;
    private OutputCollector collector;
    private List<Tuple> streamList;

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
                int medianIdx = (int)Math.round(streamList.size() / 2);
                double minScore = abnormalStreams.get(0).getDouble(1);
                double medianScore = abnormalStreams.get(medianIdx).getDouble(1);
                
                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore = streamProfile.getDouble(1);
                    boolean isAbnormal = false;
                    
                    if (streamScore > 2 * medianScore - minScore) {
                        isAbnormal = true;
                    }
                    
                    collector.emit(new Values(streamProfile.getString(0), 
                            streamProfile.getDouble(1), streamProfile.getLong(2), 
                            isAbnormal, streamProfile.getValue(3)));
                }
                
                this.streamList.clear();
            }

            this.previousTimestamp = timestamp;
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

        int medianIdx = (int)Math.round(streamList.size() / 2);
        Tuple medianTuple = BFPRT.bfprt(streamList, medianIdx);
        double minScore = Double.MAX_VALUE;
        
        for (int i = 0; i < medianIdx; ++i) {
            double score = streamList.get(i).getDouble(1);
            if (score < minScore) {
                minScore = score; 
            }
        }

        double medianScore = medianTuple.getDouble(1);

        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}