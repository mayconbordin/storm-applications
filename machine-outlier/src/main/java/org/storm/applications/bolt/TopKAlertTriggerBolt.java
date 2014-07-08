package org.storm.applications.bolt;

import java.util.ArrayList;
import java.util.Comparator;
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
import org.storm.applications.util.Sorter;

/**
 * Always trigger the top-K objects as abnormal.
 * @author yexijiang
 *
 */
public class TopKAlertTriggerBolt extends BaseRichBolt {
    private int K;
    private long previousTimestamp;
    private OutputCollector collector;
    private List<Tuple> streamList;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Object objK = stormConf.get("K");
        if (objK == null || objK.toString().trim().length() == 0) {
            K = 3;
        } else {
            K = Integer.parseInt(objK.toString());
        }
        this.collector = collector;
        this.previousTimestamp = 0;
        this.streamList = new ArrayList<Tuple>();
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLong(2);
        
        // new batch
        if (timestamp > previousTimestamp) {
            // sort the tuples in stream list
            Sorter.quicksort(this.streamList, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    double score1 = o1.getDouble(1);
                    double score2 = o2.getDouble(1);
                    if (score1 < score2) {
                        return -1;
                    } else if (score1 > score2) {
                        return 1;
                    }
                    return 0;
                }
            });

            //	treat the top-K as abnormal
            int realK = this.streamList.size() < K ? this.streamList.size() : K;
            for (int i = 0; i < this.streamList.size(); ++i) {
                Tuple streamProfile = this.streamList.get(i);
                boolean isAbnormal = false;
                
                // last three stream are marked as abnormal
                if (i >= this.streamList.size() - 3) {
                    isAbnormal = true;
                }
                this.collector.emit(new Values(streamProfile.getString(0), streamProfile.getDouble(1), streamProfile.getLong(2), isAbnormal, streamProfile.getValue(3)));
            }

            this.previousTimestamp = timestamp;
            
            // clear the cache
            this.streamList.clear();
        }

        this.streamList.add(input);
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ANOMALY_STREAM_FIELD, STREAM_ANOMALY_SCORE_FIELD, 
                TIMESTAMP_FIELD, IS_ABNORMAL_FIELD, OBSERVATION_FIELD));
    }
}