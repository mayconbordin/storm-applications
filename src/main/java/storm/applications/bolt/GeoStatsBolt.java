package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import static storm.applications.constants.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class GeoStatsBolt extends AbstractBolt {
    private Map<String, CountryStats> stats;

    @Override
    public void initialize() {
        stats = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String country = input.getStringByField(Field.COUNTRY);
        String city    = input.getStringByField(Field.CITY);
        
        if (!stats.containsKey(country)) {
            stats.put(country, new CountryStats(country));
        }
        
        stats.get(country).cityFound(city);
        
        collector.emit(input, new Values(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.COUNTRY_TOTAL, Field.CITY, Field.CITY_TOTAL);
    }
    
    private class CountryStats {
        private int countryTotal = 0;

        private static final int COUNT_INDEX = 0;
        private static final int PERCENTAGE_INDEX = 1;
        
        private final String countryName;
        private final Map<String, List<Integer>> cityStats = new HashMap<>();

        public CountryStats(String countryName) {
            this.countryName = countryName;
        }
        
        public void cityFound(String cityName) {
            countryTotal++;
            
            if (cityStats.containsKey(cityName)) {
                cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX) + 1 );
            } else {
                List<Integer> list = new LinkedList<>();
                list.add(1);
                list.add(0);
                cityStats.put(cityName, list);
            }

            double percent = (double)cityStats.get(cityName).get(COUNT_INDEX)/(double)countryTotal;
            cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
        }

        public int getCountryTotal() {
            return countryTotal;
        }

        public int getCityTotal(String cityName) {
            return cityStats.get(cityName).get(COUNT_INDEX);
        }

        @Override
        public String toString() {
            return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
                    + "Cities: " + cityStats.toString();
        }
    }
}
