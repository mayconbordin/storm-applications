package storm.applications.hooks;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.Map;
import storm.applications.metrics.MetricsFactory;
import storm.applications.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SpoutMeterHook extends BaseTaskHook {
    private Configuration config;
    private Meter emittedTuples;
    
    @Override
    public void prepare(Map conf, TopologyContext context) {
        config = Configuration.fromMap(conf);
        
        MetricRegistry registry = MetricsFactory.createRegistry(config);
        
        String componentId = context.getThisComponentId();
        String taskId      = String.valueOf(context.getThisTaskId());
        
        emittedTuples  = registry.meter(MetricRegistry.name("emitted", componentId, taskId));
    }

    @Override
    public void emit(EmitInfo info) {
        emittedTuples.mark();
    }
}
