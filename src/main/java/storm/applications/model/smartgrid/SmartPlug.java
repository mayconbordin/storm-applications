package storm.applications.model.smartgrid;

import java.util.Random;
import storm.applications.util.math.RandomUtil;

/**
 * From: http://corsi.dei.polimi.it/distsys/2013-2014/projects.html
 * @author Alessandro Sivieri
 */
public class SmartPlug {
    private final Random random = new Random();
    
    private final int id;
    private final int peakLoad;
    
    private boolean on;
    private boolean canBeChanged;
    private long since;
    private long howLong;
    
    private int loadOscillation;
    private double probabilityOn;
    private int[] onLenghts;
    private long totalLoad;

    public SmartPlug(int id, int peakLoad, int loadOscillation, double probabilityOn,
            int[] onLenghts) {
        this.peakLoad = peakLoad;
        this.id = id;
        this.loadOscillation = loadOscillation;
        this.probabilityOn = probabilityOn;
        this.onLenghts = onLenghts;
        
        this.since = 0;
        this.on = false;
        this.canBeChanged = true;
        this.totalLoad = 0l;
    }

    public boolean isOn() {
        return on;
    }

    public int getPeakLoad() {
        return peakLoad;
    }

    public int getId() {
        return id;
    }
    
    /**
     * @return Accumulated work since the start of the sensor in watt-second
     */
    public long getTotalLoad() {
        return totalLoad;
    }
    
    /**
     * @return Accumulated work since the start of the sensor in killowatt hour
     */
    public double getTotalLoadkWh() {
        // Divide by 3600000 to convert to killowatt hour
        return (double)totalLoad / 3600000.0;
    }
    
    public void tryToSetOn(long currentTimestamp) {
        if (on) {
            if (currentTimestamp - since > howLong) {
                on = false;
                since = currentTimestamp;
                howLong = onLenghts[RandomUtil.randomMinMax(0, onLenghts.length - 1)];
                canBeChanged = false;
            }
        } else {
            if (currentTimestamp - since > howLong) {
                canBeChanged = true;
            }
            if (canBeChanged && random.nextDouble() <= probabilityOn) {
                on = true;
                since = currentTimestamp;
                howLong = onLenghts[RandomUtil.randomMinMax(0, onLenghts.length - 1)];
            }
        }
    }
    
    public int getLoad() {
        if (on) {
            int min = peakLoad - loadOscillation;
            int max = peakLoad + loadOscillation;
            int load = RandomUtil.randomMinMax(min, max);
            
            totalLoad += (load * howLong);
            return load;
        }
        
        return 0;
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = 31 * result + id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SmartPlug other = (SmartPlug)obj;
        return id == other.id;
    }

    @Override
    public String toString() {
        return "plug " + id;
    }

    public String toCSV() {
        return String.valueOf(id) + "," + getLoad();
    }
}
