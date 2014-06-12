package org.storm.applications.sink;

import backtype.storm.tuple.Tuple;
import org.storm.applications.metadata.MachineMetadata;

/**
 *
 * @author mayconbordin
 */
public class MachineMetadataSink extends FileSink {

    public MachineMetadataSink(String file) {
        super(file);
    }

    @Override
    protected String formatTuple(Tuple tuple) {
        MachineMetadata metadata = (MachineMetadata) tuple.getValue(4);
        
        String line = "\"" + tuple.getValue(0) + "\"," + tuple.getValue(1) + ","
                    + tuple.getValue(2) + ",\"" + tuple.getValue(3) + "\","
                    + metadata.getCpuIdleTime() + "," + metadata.getFreeMemoryPercent();
        
        return line;
    }
    
    
}
