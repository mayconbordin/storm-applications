package org.storm.applications.predictor;

/**
 *
 * @author maycon
 */
public interface IMarkovModelSource {
    public String getModel(String key);
}
