package storm.applications.model.scorer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataInstanceScorerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DataInstanceScorerFactory.class);
    
    public static DataInstanceScorer getDataInstanceScorer(String dataTypeName) {
        switch (dataTypeName) {
            case "machineMetadata":
                return new MachineDataInstanceScorer();
                
            default:
                LOG.error("{} is not a valid data type for the Scorer", dataTypeName);
                return null;
        }
    }
}
