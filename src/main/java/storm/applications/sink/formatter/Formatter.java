package storm.applications.sink.formatter;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import storm.applications.util.config.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Formatter {
    protected Configuration config;
    protected TopologyContext context;
    
    public void initialize(Configuration config, TopologyContext context) {
        this.config = config;
        this.context = context;
    }
    
    public abstract String format(Tuple tuple);
}
