package storm.applications.hooks;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.task.TopologyContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import storm.applications.metrics.MetricsFactory;
import storm.applications.util.config.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SpoutMeterHook extends BaseTaskHook {
    private Configuration config;
    private Meter emittedTuples;
    private Timer completeLatency;
    
    @Override
    public void prepare(Map conf, TopologyContext context) {
        config = Configuration.fromMap(conf);
        
        MetricRegistry registry = MetricsFactory.createRegistry(config);
        
        String componentId = context.getThisComponentId();
        String taskId      = String.valueOf(context.getThisTaskId());
        
        emittedTuples   = registry.meter(MetricRegistry.name("emitted", componentId, taskId));
        completeLatency = registry.timer(MetricRegistry.name("complete-latency", componentId, taskId));
    }

    @Override
    public void emit(EmitInfo info) {
        emittedTuples.mark();
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
        if (info.completeLatencyMs != null) {
            completeLatency.update(info.completeLatencyMs, TimeUnit.MILLISECONDS);
        }
    }
}
