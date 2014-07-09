package storm.applications.model.fraud.predictor;

/**
 *
 * @author maycon
 */
public interface IMarkovModelSource {
    public String getModel(String key);
}
