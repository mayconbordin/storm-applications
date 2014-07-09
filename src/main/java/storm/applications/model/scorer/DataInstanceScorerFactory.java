package storm.applications.model.scorer;

public class DataInstanceScorerFactory {
    public static DataInstanceScorer getDataInstanceScorer(String dataTypeName) {
        if (dataTypeName.equals("machineMetadata")) {
            return new MachineDataInstanceScorer();
        }
        /*else if(dataTypeName.equals("twitterData")) {
                return new TwitterDataInstanceScorer();
        }*/
        try {
            throw new Exception("No matched data type scorer for " + dataTypeName);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
