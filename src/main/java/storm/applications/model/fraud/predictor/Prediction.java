package storm.applications.model.fraud.predictor;

import java.io.Serializable;

public class Prediction implements Serializable {
    private String entityId;
    private double score;
    private String[] states;
    private boolean outlier;

    public Prediction(String entityId, double score, String[] states, boolean outlier) {
        this.entityId = entityId;
        this.score = score;
        this.states = states;
        this.outlier = outlier;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String[] getStates() {
        return states;
    }

    public void setStates(String[] states) {
        this.states = states;
    }

    public boolean isOutlier() {
        return outlier;
    }

    public void setOutlier(boolean outlier) {
        this.outlier = outlier;
    }
}