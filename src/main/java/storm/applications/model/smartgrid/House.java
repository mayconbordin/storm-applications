package storm.applications.model.smartgrid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * From: http://corsi.dei.polimi.it/distsys/2013-2014/projects.html
 * @author Alessandro Sivieri
 */
public class House {
    private final Map<Integer, Household> households;
    private final int id;

    public House(int id) {
        this.id = id;
        this.households = new HashMap<>();
    }

    public int getId() {
        return id;
    }

    public List<Household> getHouseholds() {
        return new ArrayList<>(households.values());
    }

    public void addHousehold(Household household) {
        households.put(household.getId(), household);
    }

    public List<String> toCSV() {
        ArrayList<String> res = new ArrayList<>();
        for (Household household : households.values()) {
            List<String> plugs = household.toCSV();
            for (String householdplug : plugs) {
                res.add(String.valueOf(id) + "," + householdplug);
            }
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
        House other = (House)obj;
        return id == other.id;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("house " + id + " {");
        for (Household household : households.values()) {
            res.append(household).append(", ");
        }
        res.append("}");
        return res.toString();
    }
}