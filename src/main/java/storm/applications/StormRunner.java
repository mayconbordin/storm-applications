package storm.applications;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.topology.AdsAnalyticsTopology;
import storm.applications.topology.BargainIndexTopology;
import storm.applications.topology.ClickAnalyticsTopology;
import storm.applications.topology.FraudDetectionTopology;
import storm.applications.topology.LinearRoadTopology;
import storm.applications.topology.LogProcessingTopology;
import storm.applications.topology.MachineOutlierTopology;
import storm.applications.topology.ReinforcementLearnerTopology;
import storm.applications.topology.SentimentAnalysisTopology;
import storm.applications.topology.SpamFilterTopology;
import storm.applications.topology.SpikeDetectionTopology;
import storm.applications.topology.TrafficMonitoringTopology;
import storm.applications.topology.TrendingTopicsTopology;
import storm.applications.topology.VoIPSTREAMTopology;
import storm.applications.topology.WordCountTopology;
import storm.applications.util.config.Configuration;

/**
 * Utility class to run a Storm topology
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StormRunner {
    private static final Logger LOG = LoggerFactory.getLogger(StormRunner.class);
    private static final String RUN_LOCAL  = "local";
    private static final String RUN_REMOTE = "remote";
    private static final String CFG_PATH = "/config/%s.properties";
    
    @Parameter
    public List<String> parameters = Lists.newArrayList();
    
    @Parameter(names = {"-m", "--mode"}, description = "Mode for running the topology")
    public String mode = "local";
    
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = true)
    public String application;
    
    @Parameter(names = {"-t", "--topology-name"}, required = false, description = "The name of the topology")
    public String topologyName;
    
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;
    
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the topology (local mode only)")
    public int runtimeInSeconds = 300;
    
    private final AppDriver driver;
    private Config config;

    public StormRunner() {
        driver = new AppDriver();
        
        driver.addApp("ads-analytics"        , AdsAnalyticsTopology.class);
        driver.addApp("bargain-index"        , BargainIndexTopology.class);
        driver.addApp("click-analytics"      , ClickAnalyticsTopology.class);
        driver.addApp("fraud-detection"      , FraudDetectionTopology.class);
        driver.addApp("linear-road"          , LinearRoadTopology.class);
        driver.addApp("log-processing"       , LogProcessingTopology.class);
        driver.addApp("machine-outlier"      , MachineOutlierTopology.class);
        driver.addApp("reinforcement-learner", ReinforcementLearnerTopology.class);
        driver.addApp("sentiment-analysis"   , SentimentAnalysisTopology.class);
        driver.addApp("spam-filter"          , SpamFilterTopology.class);
        driver.addApp("spike-detection"      , SpikeDetectionTopology.class);
        driver.addApp("trending-topics"      , TrendingTopicsTopology.class);
        driver.addApp("voipstream"           , VoIPSTREAMTopology.class);
        driver.addApp("word-count"           , WordCountTopology.class);
        driver.addApp("traffic-monitoring"   , TrafficMonitoringTopology.class);
    }
    
    public void run() throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        // Loads the configuration file set by the user or the default configuration
        try {
            // load default configuration
            if (configStr == null) {
                String cfg = String.format(CFG_PATH, application);
                Properties p = loadProperties(cfg, (configStr == null));
            
                config = Configuration.fromProperties(p);
                LOG.info("Loaded default configuration file {}", cfg);
            } else {
                config = Configuration.fromStr(configStr);
                LOG.info("Loaded configuration from command line argument");
            }
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            throw new RuntimeException("Unable to load configuration file", ex);
        }
        
        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name "+application+" is invalid");
        }
        
        // In case no topology names is given, create one
        if (topologyName == null) {
            topologyName = String.format("%s-%d", application, new Random().nextInt());
        }
        
        // Get the topology and execute on Storm
        StormTopology stormTopology = app.getTopology(topologyName, config);
        
        switch (mode) {
            case RUN_LOCAL:
                runTopologyLocally(stormTopology, topologyName, config, runtimeInSeconds);
                break;
            case RUN_REMOTE:
                runTopologyRemotely(stormTopology, topologyName, config);
                break;
            default:
                throw new RuntimeException("Valid running modes are 'local' and 'remote'");
        }
    }
    
    public static void main(String[] args) throws Exception {
        StormRunner runner = new StormRunner();
        JCommander cmd = new JCommander(runner);
        
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
            System.exit(1);
        }
        
        try {
            runner.run();
        } catch (AlreadyAliveException | InvalidTopologyException ex) {
            LOG.error("Error in running topology remotely", ex);
        } catch (InterruptedException ex) {
            LOG.error("Error in running topology locally", ex);
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
        LOG.info("Starting Storm on local mode to run for {} seconds", runtimeInSeconds);
        LocalCluster cluster = new LocalCluster();
        
        LOG.info("Topology {} submitted", topologyName);
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * 1000);
        
        cluster.killTopology(topologyName);
        LOG.info("Topology {} finished", topologyName);
        
        cluster.shutdown();
        LOG.info("Local Storm cluster was shutdown", topologyName);
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
    
    public static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;
        
        if (classpath) {
            is = StormRunner.class.getResourceAsStream(filename);
        } else {
            is = new FileInputStream(filename);
        }
        
        properties.load(is);
        is.close();
        
        return properties;
    }
}
