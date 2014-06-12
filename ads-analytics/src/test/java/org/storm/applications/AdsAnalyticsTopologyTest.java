package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopologyTest {
    
    public AdsAnalyticsTopologyTest() {
    }

    /**
     * Test of buildTopology method, of class AdsAnalyticsTopology.
     */
    @Test
    public void testBuildTopology() throws InterruptedException {
        Config config = new Config();
        config.setDebug(true);

        AdsAnalyticsTopology adsAnalytics = new AdsAnalyticsTopology("adsAnalyticsTopology", config);
        
        StormTopology topology = adsAnalytics.buildTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(adsAnalytics.getTopologyName(), config, topology);
        
        Thread.sleep(1000000);
        
        cluster.killTopology(adsAnalytics.getTopologyName());
    }
    
}
