package storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.topology.AbstractTopology;

/**
 *
 * @author mayconbordin
 */
public class AppDriver {
    private static final Logger LOG = LoggerFactory.getLogger(AppDriver.class);
    private final Map<String, AppDescriptor> applications;

    public AppDriver() {
        applications = new HashMap<>();
    }
    
    public void addApp(String name, Class<? extends AbstractTopology> cls) {
        applications.put(name, new AppDescriptor(cls));
    }
    
    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }
    
    public static class AppDescriptor {
        private final Class<? extends AbstractTopology> cls;

        public AppDescriptor(Class<? extends AbstractTopology> cls) {
            this.cls = cls;
        }
        
        public StormTopology getTopology(String topologyName, Config config) {
            try {
                Constructor c = cls.getConstructor(String.class, Config.class);
                LOG.info("Loaded topology {}", cls.getCanonicalName());

                AbstractTopology topology = (AbstractTopology) c.newInstance(topologyName, config);
                topology.initialize();
                return topology.buildTopology();
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load topology class", ex);
                return null;
            }
        }
    }
}
