package storm.applications.hooks;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.task.TopologyContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static storm.applications.constants.BaseConstants.BaseField.*;
import storm.applications.metrics.MetricsFactory;
import storm.applications.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SinkMeterHook extends BaseTaskHook {
    private Configuration config;
    
    private Meter receivedTuples;
    private Timer processLatency;
    private Timer overallLatency;
    
    @Override
    public void prepare(Map conf, TopologyContext context) {
        config = Configuration.fromMap(conf);
        
        MetricRegistry registry = MetricsFactory.createRegistry(config);
        
        String componentId = context.getThisComponentId();
        String taskId      = String.valueOf(context.getThisTaskId());

        receivedTuples = registry.meter(MetricRegistry.name("received", componentId, taskId));
        processLatency = registry.timer(MetricRegistry.name("process_latency", componentId, taskId));
        overallLatency = registry.timer(MetricRegistry.name("overall_latency", componentId, taskId));
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
        long now = System.currentTimeMillis();
        receivedTuples.mark();
        
        if (info.tuple != null && info.tuple.contains(CREATED_AT)) {
            long createdAt = info.tuple.getLongByField(CREATED_AT);
            overallLatency.update(now - createdAt, TimeUnit.MILLISECONDS);
        }

        if (info.executeLatencyMs != null) {
            processLatency.update(info.executeLatencyMs, TimeUnit.MILLISECONDS);
        }
    }
}
