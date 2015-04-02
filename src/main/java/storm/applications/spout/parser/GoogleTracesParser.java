package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import java.util.List;
import storm.applications.model.metadata.MachineMetadata;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GoogleTracesParser extends Parser {
    private static final int TIMESTAMP  = 0;
    private static final int MACHINE_ID = 4;
    private static final int CPU        = 5;
    private static final int MEMORY     = 6;
    
    @Override
    public List<StreamValues> parse(String str) {
        String[] items = str.split(",");
        
        if (items.length != 19)
            return null;
        
        String id      = items[MACHINE_ID];
        long timestamp = Long.parseLong(items[TIMESTAMP]);
        double cpu     = Double.parseDouble(items[CPU]) * 10;
        double memory  = Double.parseDouble(items[MEMORY]) * 10;
        int msgId = String.format("%s:%s", id, timestamp).hashCode();
        
        StreamValues values = new StreamValues();
        values.add(id);
        values.add(timestamp);
        values.add(new MachineMetadata(timestamp, id, cpu, memory));
        values.setMessageId(msgId);
        
        return ImmutableList.of(values);
    }
}
