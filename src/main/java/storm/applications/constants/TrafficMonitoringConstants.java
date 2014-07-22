package storm.applications.constants;

public interface TrafficMonitoringConstants extends BaseConstants {
    String PREFIX = "tm";
    
    interface Conf extends BaseConf {
        String MAP_MATCHER_SHAPEFILE = "tm.map_matcher.shapefile";
        String MAP_MATCHER_THREADS = "tm.map_matcher.threads";
        String MAP_MATCHER_LAT_MIN = "tm.map_matcher.lat.min";
        String MAP_MATCHER_LAT_MAX = "tm.map_matcher.lat.max";
        String MAP_MATCHER_LON_MIN = "tm.map_matcher.lon.min";
        String MAP_MATCHER_LON_MAX = "tm.map_matcher.lon.max";
        
        String SPEED_CALCULATOR_THREADS = "tm.speed_calculator.threads";
        
        String ROAD_FEATURE_ID_KEY    = "tm.road.feature.id_key";
        String ROAD_FEATURE_WIDTH_KEY = "tm.road.feature.width_key";
    }
    
    interface Component extends BaseComponent {
        String MAP_MATCHER = "mapMatcherBolt";
        String SPEED_CALCULATOR = "speedCalculatorBolt";
    }
    
    interface Field {
        String VEHICLE_ID = "vehicleID";
        String DATE_TIME  = "dateTime";
        String OCCUPIED   = "occupied";
        String SPEED      = "speed";
        String BEARING    = "bearing";
        String LATITUDE   = "latitude";
        String LONGITUDE  = "longitude";
        String ROAD_ID    = "roadID";
        String NOW_DATE   = "nowDate";
        String AVG_SPEED  = "averageSpeed";
        String COUNT      = "count";
    }
}
