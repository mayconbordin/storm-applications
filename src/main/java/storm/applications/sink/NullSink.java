package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class NullSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(NullSink.class);
    
    @Override
    public void execute(Tuple input) {
        // do nothing
        collector.ack(input);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
}
