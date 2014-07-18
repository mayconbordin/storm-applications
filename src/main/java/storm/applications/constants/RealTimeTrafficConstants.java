package storm.applications.constants;

public interface RealTimeTrafficConstants extends BaseConstants {
    String PREFIX = "rt";
    
    interface Conf extends BaseConf {
        String MAP_MATCHER_SHAPEFILE = "rt.map_matcher.shapefile";
        String MAP_MATCHER_THREADS = "rt.map_matcher.threads";
        String MAP_MATCHER_LAT_MIN = "rt.map_matcher.lat.min";
        String MAP_MATCHER_LAT_MAX = "rt.map_matcher.lat.max";
        String MAP_MATCHER_LON_MIN = "rt.map_matcher.lon.min";
        String MAP_MATCHER_LON_MAX = "rt.map_matcher.lon.max";
        
        String SPEED_CALCULATOR_THREADS = "rt.speed_calculator.threads";
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
