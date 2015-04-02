package storm.applications.bolt;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;
import storm.applications.util.sort.Sorter;

/**
 * Always trigger the top-K objects as abnormal.
 * @author yexijiang
 *
 */
public class TopKAlertTriggerBolt extends AbstractBolt {
    private int k;
    private long previousTimestamp;
    private List<Tuple> streamList;

    @Override
    public void initialize() {
        k = config.getInt(Conf.ALERT_TRIGGER_TOPK, 3);
        previousTimestamp = 0;
        streamList = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(Field.TIMESTAMP);
        
        // new batch
        if (timestamp > previousTimestamp) {
            // sort the tuples in stream list
            Sorter.quicksort(streamList, new Comparator<Tuple>() {
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
            int realK = streamList.size() < k ? streamList.size() : k;
            for (int i = 0; i < streamList.size(); ++i) {
                Tuple streamProfile = streamList.get(i);
                boolean isAbnormal = false;
                
                // last three stream are marked as abnormal
                if (i >= streamList.size() - 3) {
                    isAbnormal = true;
                }
                collector.emit(streamList, new Values(streamProfile.getString(0), streamProfile.getDouble(1), streamProfile.getLong(2), isAbnormal, streamProfile.getValue(3)));
            }

            previousTimestamp = timestamp;
            
            // clear the cache
            streamList.clear();
        }

        streamList.add(input);
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ANOMALY_STREAM, Field.STREAM_ANOMALY_SCORE, 
                Field.TIMESTAMP, Field.IS_ABNORMAL, Field.OBSERVATION);
    }
}