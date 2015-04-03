package storm.applications.model.smartgrid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * From: http://corsi.dei.polimi.it/distsys/2013-2014/projects.html
 * @author Alessandro Sivieri
 */
public class Household {
    private final Map<Integer, SmartPlug> plugs;
    private final int id;

    public Household(int id) {
        this.id = id;
        this.plugs = new HashMap<>();
    }

    public int getId() {
        return id;
    }

    public List<SmartPlug> getPlugs() {
        return new ArrayList<>(plugs.values());
    }

    public void addSmartPlug(SmartPlug plug) {
        plugs.put(plug.getId(), plug);
    }

    public List<String> toCSV() {
        ArrayList<String> res = new ArrayList<>();
        for (SmartPlug plug : plugs.values()) {
            res.add(String.valueOf(id) + "," + plug.toCSV());
        }
        return res;
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        Household other = (Household)obj;
        return id == other.id;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("household " + id + " {");
        for (SmartPlug plug : plugs.values()) {
            res.append(plug).append(", ");
        }
        res.append("}");
        return res.toString();
    }
}