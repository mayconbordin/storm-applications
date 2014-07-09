package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.bolt.ACDBolt;
import storm.applications.bolt.CTBolt;
import storm.applications.bolt.ECRBolt;
import storm.applications.bolt.ENCRBolt;
import storm.applications.bolt.FoFiRBolt;
import storm.applications.bolt.GlobalACDBolt;
import storm.applications.bolt.RCRBolt;
import storm.applications.bolt.ScoreBolt;
import storm.applications.bolt.URLBolt;
import storm.applications.bolt.VariationDetectorBolt;
import storm.applications.sink.FileSink;
import storm.applications.spout.CDRGeneratorSpout;
import storm.applications.util.ConfigUtility;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAMTopology extends AbstractTopology {
    private int spoutThreads;
    private int varDetectThreads;
    private int ecrThreads;
    private int rcrThreads;
    private int encrThreads;
    private int ecr24Threads;
    private int ct24Threads;
    private int fofirThreads;
    private int urlThreads;
    private int globalAcdThreads;
    private int acdThreads;
    private int scorerThreads;
    private int sinkThreads;
    private String spoutType;
    private String spoutPath;

    public VoIPSTREAMTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void prepare() {
        spoutThreads     = ConfigUtility.getInt(config, "voipstream.spout.threads");
        varDetectThreads = ConfigUtility.getInt(config, "voipstream.vardetect.threads");
        ecrThreads       = ConfigUtility.getInt(config, "voipstream.ecr.threads");
        rcrThreads       = ConfigUtility.getInt(config, "voipstream.rcr.threads");
        encrThreads      = ConfigUtility.getInt(config, "voipstream.encr.threads");
        ecr24Threads     = ConfigUtility.getInt(config, "voipstream.ecr24.threads");
        ct24Threads      = ConfigUtility.getInt(config, "voipstream.ct24.threads");
        fofirThreads     = ConfigUtility.getInt(config, "voipstream.fofir.threads");
        urlThreads       = ConfigUtility.getInt(config, "voipstream.url.threads");
        globalAcdThreads = ConfigUtility.getInt(config, "voipstream.globalacd.threads");
        acdThreads       = ConfigUtility.getInt(config, "voipstream.acd.threads");
        scorerThreads    = ConfigUtility.getInt(config, "voipstream.scorer.threads");
        sinkThreads      = ConfigUtility.getInt(config, "voipstream.sink.threads");
        spoutType        = ConfigUtility.getString(config, "voipstream.spout.type");
        spoutPath        = ConfigUtility.getString(config, "voipstream.sink.path");
    }
    
    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();

        if (spoutType.equals(SPOUT_GENERATOR))
            builder.setSpout(CDR_SPOUT, new CDRGeneratorSpout(), spoutThreads);

        
        builder.setBolt(VARIATION_DETECTOR_BOLT, new VariationDetectorBolt(), varDetectThreads)
               .fieldsGrouping(CDR_SPOUT, new Fields(CALLING_NUM_FIELD, CALLED_NUM_FIELD));
        
        // Filters

        builder.setBolt(ECR_BOLT, new ECRBolt("ecr"), ecrThreads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(RCR_BOLT, new RCRBolt(), rcrThreads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, BACKUP_STREAM, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLED_NUM_FIELD));
        
        builder.setBolt(ENCR_BOLT, new ENCRBolt(), encrThreads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(ECR24_BOLT, new ECRBolt("ecr24"), ecr24Threads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
 
        builder.setBolt(CT24_BOLT, new CTBolt("ct24"), ct24Threads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        
        // Modules
        
        builder.setBolt(FOFIR_BOLT, new FoFiRBolt(), fofirThreads)
               .fieldsGrouping(RCR_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(ECR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        
        builder.setBolt(URL_BOLT, new URLBolt(), urlThreads)
               .fieldsGrouping(ENCR_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(ECR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        // the average must be global, so there must be a single instance doing that
        // perhaps a separate bolt, or if multiple bolts are used then a merger should
        // be employed at the end point.
        builder.setBolt(GLOBAL_ACD_BOLT, new GlobalACDBolt(), globalAcdThreads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(ACD_BOLT, new ACDBolt(), acdThreads)
               .fieldsGrouping(ECR24_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(CT24_BOLT, new Fields(CALLING_NUM_FIELD))
               .allGrouping(GLOBAL_ACD_BOLT);
        
        
        // Score
        builder.setBolt(SCORER_BOLT, new ScoreBolt(), scorerThreads)
               .fieldsGrouping(FOFIR_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(URL_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(ACD_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(FILE_SINK, new FileSink(spoutPath), sinkThreads)
               .fieldsGrouping(SCORER_BOLT, new Fields(CALLING_NUM_FIELD));
        
        return builder.createTopology();
    }
}
