package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.AlertTriggerBolt;
import storm.applications.bolt.ObservationScoreBolt;
import storm.applications.bolt.SlidingWindowStreamAnomalyScoreBolt;
import static storm.applications.constants.MachineOutlierConstants.*;

public class MachineOutlierTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlierTopology.class);
    
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;

    public MachineOutlierTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        scorerThreads        = config.getInt(Conf.SCORER_THREADS, 1);
        anomalyScorerThreads = config.getInt(Conf.ANOMALY_SCORER_THREADS, 1);
        alertTriggerThreads  = config.getInt(Conf.ALERT_TRIGGER_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.ID, Field.TIMESTAMP, Field.OBSERVATION));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.SCORER, new ObservationScoreBolt(), scorerThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.ANOMALY_SCORER, new SlidingWindowStreamAnomalyScoreBolt(), anomalyScorerThreads)
               .fieldsGrouping(Component.SCORER, new Fields(Field.ID));

        builder.setBolt(Component.ALERT_TRIGGER, new AlertTriggerBolt(), alertTriggerThreads)
               .shuffleGrouping(Component.ANOMALY_SCORER);
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.ALERT_TRIGGER);

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
