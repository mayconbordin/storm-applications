package storm.applications.sink;

import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import storm.applications.bolt.AbstractBolt;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.constants.BaseConstants.BaseConst;
import storm.applications.sink.formatter.Formatter;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;


public abstract class BaseSink extends AbstractBolt {
    protected String configPrefix = BaseConst.DEFAULT_CONFIG_PREFIX;
    protected Formatter formatter;
    
    @Override
    public void initialize() {
        String formatterClass = ConfigUtility.getString(config, getConfigKey(BaseConf.SINK_FORMATTER));
        formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
    }
    
    @Override
    public Fields getDefaultFields() {
        return new Fields("");
    }
    
    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
    
    protected abstract Logger getLogger();
}
