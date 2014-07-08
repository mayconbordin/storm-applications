package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.storm.applications.topology.AbstractTopology;

/**
 * Utility class to run a Storm topology
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StormRunner {
    private static final Logger LOG = LoggerFactory.getLogger(StormRunner.class);
    private static final String RUN_LOCAL  = "local";
    private static final String RUN_REMOTE = "remote";
    
    /**
     * 
     * @param args <run-mode>           The mode in which the topology will run: local or remote
     *             <topology-class>     The full name of the topology class to be executed
     *             <config-file>        The full path to the configuration file (.properties)
     *            [<runtimeInSeconds>   For how much time the topology will run (local only)]
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            LOG.error("Usage: ... <run-mode> <topology-class> <topology-name> <config-file> [<runtimeInSeconds>]");
            System.exit(1);
        }
        
        String runMode       = args[0];
        String topologyClass = args[1];
        String topologyName  = args[2];
        String configFile    = args[3];
        
        Config config               = null;
        StormTopology stormTopology = null;
        int runtimeInSeconds       = 300;
        
        if (args.length > 4) {
            runtimeInSeconds = Integer.parseInt(args[4]);
        }
        
        try {
            config = loadConfig(configFile);
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            System.exit(2);
        }
        
        try {
            Constructor c = Class.forName(topologyClass).getConstructor(String.class, Config.class);
            LOG.info("Loaded topology {}", topologyClass);
            
            AbstractTopology topology = (AbstractTopology) c.newInstance(topologyName, config);
            topology.prepare();
            stormTopology = topology.buildTopology();
        } catch (ReflectiveOperationException ex) {
            LOG.error("Unable to load topology class", ex);
            System.exit(2);
        }
        
        if (runMode.equals(RUN_LOCAL)) {
            runTopologyLocally(stormTopology, topologyName, config, runtimeInSeconds);
        } else if (runMode.equals(RUN_REMOTE)) {
            runTopologyRemotely(stormTopology, topologyName, config);
        } else {
            LOG.error("Valid running modes are 'local' and 'remote'");
        }
    }
    
    /**
     * Run the topology locally
     * @param topology The topology to be executed
     * @param topologyName The name of the topology
     * @param conf The configurations for the execution
     * @param runtimeInSeconds For how much time the topology will run
     * @throws InterruptedException 
     */
    public static void runTopologyLocally(StormTopology topology, String topologyName,
            Config conf, int runtimeInSeconds) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * 1000);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    /**
     * Run the topology remotely
     * @param topology The topology to be executed
     * @param topologyName The name of the topology
     * @param conf The configurations for the execution
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException 
     */
    public static void runTopologyRemotely(StormTopology topology, String topologyName,
            Config conf) throws AlreadyAliveException, InvalidTopologyException {
        StormSubmitter.submitTopology(topologyName, conf, topology);
    }
    
    /**
     * Creates a {@link Config} object by reading the properties file located at
     * the fileName path.
     * @param fileName The full path to the properties file
     * @return the configuration file with the key/value pairs loaded
     * @throws IOException 
     */
    public static Config loadConfig(String fileName) throws IOException {
        Config config = new Config();
        Properties properties = new Properties();
        
        InputStream is = new FileInputStream(fileName);
        properties.load(is);
        is.close();
        
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            config.put(key, value);
        }
        
        return null;
    }
}
