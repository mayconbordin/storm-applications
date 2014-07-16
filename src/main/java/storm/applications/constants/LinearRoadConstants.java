package storm.applications.constants;

/**
 *
 * @author mayconbordin
 */
public interface LinearRoadConstants extends BaseConstants {
    interface Field {
        String TIMESTAMP = "timestamp";
        String VEHICLE_ID = "vehicleId";
        String SPEED = "speed";
        String EXPRESSWAY = "expressway";
        String LANE = "lane";
        String DIRECTION = "direction";
        String SEGMENT = "segment";
        String POSITION = "position";
    }
    
    interface Conf extends BaseConf {
    }
    
    interface Component extends BaseComponent {
        
    }
}
