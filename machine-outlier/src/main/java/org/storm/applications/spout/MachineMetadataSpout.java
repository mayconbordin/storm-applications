package org.storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.Random;
import static org.storm.applications.MachineOutlierConstants.*;
import org.storm.applications.metadata.MachineMetadata;

/**
 *
 * @author mayconbordin
 */
public class MachineMetadataSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random rand;
    
    private int numMachines = 100;
    private int count = 0;
    private long currentTimestamp = 0;
    private String[] ipAddresses;
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP_FIELD, IP_FIELD, METADATA_FIELD));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        rand = new Random(System.currentTimeMillis());
        ipAddresses = new String[numMachines];
        
        for (int i=0; i<numMachines; i++) {
            ipAddresses[i] = getRandomIP();
        }
    }

    public void nextTuple() {
        if (count % numMachines == 0) {
            currentTimestamp = System.currentTimeMillis();
        }
        
        String ip = ipAddresses[count++ % numMachines];
        
        MachineMetadata metadata = new MachineMetadata();
        metadata.setTimestamp(currentTimestamp);
        metadata.setMachineIP(ip);
        metadata.setFreeMemoryPercent(getRandomBetween(0, 100));
        metadata.setCpuIdleTime(getRandomBetween(0, 100));
        
        collector.emit(new Values(metadata.getTimestamp(), metadata.getMachineIP(), metadata));
    }
    
    private double getRandomBetween(double min, double max) {
        return min + rand.nextDouble() * (max - min);
    }
    
    private String getRandomIP() {
        return rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);
    }
    
}
