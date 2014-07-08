package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import static org.storm.applications.MachineOutlierConstants.*;
import org.storm.applications.bolt.AlertTriggerBolt;
import org.storm.applications.bolt.ObservationScoreBolt;
import org.storm.applications.bolt.SlidingWindowStreamAnomalyScoreBolt;
import org.storm.applications.sink.MachineMetadataSink;
import org.storm.applications.spout.MachineMetadataSpout;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ConfigUtility;

public class MachineOutlierTopology extends AbstractTopology {
    private int spoutThreads;
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;
    private int sinkThreads;
    private String spoutPath;

    public MachineOutlierTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void prepare() {
        spoutThreads   = ConfigUtility.getInt(config, "machineoutlier.spout.threads");
        scorerThreads  = ConfigUtility.getInt(config, "machineoutlier.scorer.threads");
        anomalyScorerThreads  = ConfigUtility.getInt(config, "machineoutlier.anomalyscorer.threads");
        alertTriggerThreads   = ConfigUtility.getInt(config, "machineoutlier.alerttrigger.threads");
        sinkThreads  = ConfigUtility.getInt(config, "machineoutlier.sink.threads");
        spoutPath    = ConfigUtility.getString(config, "machineoutlier.sink.path");
    }
    
    public StormTopology buildTopology() {
        builder.setSpout(METADATA_SPOUT, new MachineMetadataSpout(), spoutThreads);
        
        builder.setBolt(SCORER_BOLT, new ObservationScoreBolt("machineMetadata"), scorerThreads)
               .shuffleGrouping(METADATA_SPOUT);
        
        builder.setBolt(ANOMALY_SCORER_BOLT, new SlidingWindowStreamAnomalyScoreBolt(), anomalyScorerThreads)
               .fieldsGrouping(SCORER_BOLT, new Fields(ID_FIELD));

        builder.setBolt(ALERT_TRIGGER_BOLT, new AlertTriggerBolt(), alertTriggerThreads)
               .shuffleGrouping(ANOMALY_SCORER_BOLT);
        
        builder.setBolt(FILE_SINK, new MachineMetadataSink(spoutPath), sinkThreads)
               .shuffleGrouping(ALERT_TRIGGER_BOLT);
        
        return builder.createTopology();
    }
}
