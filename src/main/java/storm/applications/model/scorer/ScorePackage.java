package storm.applications.model.scorer;

/**
 * ScorePackage contains the score as well as the object.
 * @author yexijiang
 *
 * @param <T>
 */
public class ScorePackage<T> {
    private String id;
    private double score;
    private T obj;

    public ScorePackage(String id, double score, T obj) {
        super();
        this.id = id;
        this.score = score;
        this.obj = obj;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }
}
