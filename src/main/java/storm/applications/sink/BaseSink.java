package storm.applications.sink;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import storm.applications.bolt.AbstractBolt;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.sink.formatter.Formatter;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;


public abstract class BaseSink extends AbstractBolt {
    protected String formatterKey = BaseConf.SINK_FORMATTER;
    protected Formatter formatter;
    
    @Override
    public void initialize() {
        String formatterClass = ConfigUtility.getString(config, formatterKey);
        formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    public void setFormatterKey(String formatterKey) {
        this.formatterKey = formatterKey;
    }
    
    protected abstract Logger getLogger();
}
