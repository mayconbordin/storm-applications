package storm.applications.hooks;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import storm.applications.metrics.MetricsFactory;
import storm.applications.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class BoltMeterHook extends BaseTaskHook {
    private Configuration config;
    
    private Meter emittedTuples;
    private Meter receivedTuples;
    private Timer processLatency;
    
    @Override
    public void prepare(Map conf, TopologyContext context) {
        config = Configuration.fromMap(conf);
        
        MetricRegistry registry = MetricsFactory.createRegistry(config);
        
        String componentId = context.getThisComponentId();
        String taskId      = String.valueOf(context.getThisTaskId());
        
        emittedTuples  = registry.meter(MetricRegistry.name("emitted", componentId, taskId));
        receivedTuples = registry.meter(MetricRegistry.name("received", componentId, taskId));
        processLatency = registry.timer(MetricRegistry.name("process_latency", componentId, taskId));
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
        receivedTuples.mark();
        
        if (info.executeLatencyMs != null) {
            processLatency.update(info.executeLatencyMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void emit(EmitInfo info) {
        emittedTuples.mark();
    }
}
