package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAMTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(VoIPSTREAMTopology.class);
    
    private int varDetectThreads;
    private int ecrThreads;
    private int rcrThreads;
    private int encrThreads;
    private int ecr24Threads;
    private int ct24Threads;
    private int fofirThreads;
    private int urlThreads;
    private int acdThreads;
    private int scorerThreads;

    public VoIPSTREAMTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        varDetectThreads = config.getInt(Conf.VAR_DETECT_THREADS, 1);
        ecrThreads       = config.getInt(Conf.ECR_THREADS, 1);
        rcrThreads       = config.getInt(Conf.RCR_THREADS, 1);
        encrThreads      = config.getInt(Conf.ENCR_THREADS, 1);
        ecr24Threads     = config.getInt(Conf.ECR24_THREADS, 1);
        ct24Threads      = config.getInt(Conf.CT24_THREADS, 1);
        fofirThreads     = config.getInt(Conf.FOFIR_THREADS, 1);
        urlThreads       = config.getInt(Conf.URL_THREADS, 1);
        acdThreads       = config.getInt(Conf.ACD_THREADS, 1);
        scorerThreads    = config.getInt(Conf.SCORER_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.RECORD));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.VARIATION_DETECTOR, new VariationDetectorBolt(), varDetectThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.CALLING_NUM, Field.CALLED_NUM));
        
        // Filters

        builder.setBolt(Component.ECR, new ECRBolt("ecr"), ecrThreads)
               .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));
        
        builder.setBolt(Component.RCR, new RCRBolt(), rcrThreads)
               .fieldsGrouping(Component.VARIATION_DETECTOR, Stream.BACKUP, new Fields(Field.CALLING_NUM))
               .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLED_NUM));
        
        builder.setBolt(Component.ENCR, new ENCRBolt(), encrThreads)
               .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));
        
        builder.setBolt(Component.ECR24, new ECRBolt("ecr24"), ecr24Threads)
               .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));
 
        builder.setBolt(Component.CT24, new CTBolt("ct24"), ct24Threads)
               .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));
        
        
        // Modules
        
        builder.setBolt(Component.FOFIR, new FoFiRBolt(), fofirThreads)
               .fieldsGrouping(Component.RCR, new Fields(Field.CALLING_NUM))
               .fieldsGrouping(Component.ECR, new Fields(Field.CALLING_NUM));
        
        
        builder.setBolt(Component.URL, new URLBolt(), urlThreads)
               .fieldsGrouping(Component.ENCR, new Fields(Field.CALLING_NUM))
               .fieldsGrouping(Component.ECR, new Fields(Field.CALLING_NUM));
        
        // the average must be global, so there must be a single instance doing that
        // perhaps a separate bolt, or if multiple bolts are used then a merger should
        // be employed at the end point.
        builder.setBolt(Component.GLOBAL_ACD, new GlobalACDBolt(), 1)
               .allGrouping(Component.VARIATION_DETECTOR);
        
        builder.setBolt(Component.ACD, new ACDBolt(), acdThreads)
               .fieldsGrouping(Component.ECR24, new Fields(Field.CALLING_NUM))
               .fieldsGrouping(Component.CT24, new Fields(Field.CALLING_NUM))
               .allGrouping(Component.GLOBAL_ACD);
        
        
        // Score
        builder.setBolt(Component.SCORER, new ScoreBolt(), scorerThreads)
               .fieldsGrouping(Component.FOFIR, new Fields(Field.CALLING_NUM))
               .fieldsGrouping(Component.URL, new Fields(Field.CALLING_NUM))
               .fieldsGrouping(Component.ACD, new Fields(Field.CALLING_NUM));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .fieldsGrouping(Component.SCORER, new Fields(Field.CALLING_NUM));
        
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
