package storm.applications.topology;

import backtype.storm.Config;
import storm.applications.constants.BaseConstants;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class BasicTopology extends AbstractTopology {
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected int spoutThreads;
    protected int sinkThreads;
    
    public BasicTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        String spoutClass = ConfigUtility.getString(config, BaseConf.SPOUT_CLASS);
        spout = (AbstractSpout) ClassLoaderUtils.newInstance(spoutClass, "spout", getLogger());
        
        String sinkClass = ConfigUtility.getString(config, BaseConf.SINK_CLASS);
        sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        
        spoutThreads = ConfigUtility.getInt(config, BaseConf.SPOUT_THREADS, 1);
        sinkThreads  = ConfigUtility.getInt(config, BaseConf.SINK_THREADS, 1);
    }
}
