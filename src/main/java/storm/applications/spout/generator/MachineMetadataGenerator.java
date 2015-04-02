package storm.applications.spout.generator;

import java.util.Random;
import storm.applications.constants.MachineOutlierConstants.Conf;
import storm.applications.model.metadata.MachineMetadata;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MachineMetadataGenerator extends Generator {
    private Random rand;
    
    private int numMachines;
    private int count = 0;
    private long currentTimestamp = 0;
    private String[] ipAddresses;
    
    @Override
    public StreamValues generate() {
        if (count % numMachines == 0) {
            currentTimestamp = System.currentTimeMillis();
        }
        
        String ip = ipAddresses[count++ % numMachines];
        
        MachineMetadata metadata = new MachineMetadata();
        metadata.setTimestamp(currentTimestamp);
        metadata.setMachineIP(ip);
        metadata.setFreeMemoryPercent(getRandomBetween(0, 100));
        metadata.setCpuIdleTime(getRandomBetween(0, 100));
        
        return new StreamValues(metadata.getTimestamp(), metadata.getMachineIP(), metadata);
    }

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        numMachines = config.getInt(Conf.GENERATOR_NUM_MACHINES, 100);
        
        rand = new Random(System.currentTimeMillis());
        ipAddresses = new String[numMachines];
        
        for (int i=0; i<numMachines; i++) {
            ipAddresses[i] = getRandomIP();
        }
    }
    
    private double getRandomBetween(double min, double max) {
        return min + rand.nextDouble() * (max - min);
    }
    
    private String getRandomIP() {
        return rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);
    }
}
