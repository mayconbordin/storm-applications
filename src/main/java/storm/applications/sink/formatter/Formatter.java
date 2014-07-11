package storm.applications.sink.formatter;

import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Formatter {
    protected Map config;

    public void initialize(Map config) {
        this.config = config;
    }
    
    public abstract String format(Tuple tuple);
}
