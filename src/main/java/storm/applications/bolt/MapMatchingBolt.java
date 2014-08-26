package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.TrafficMonitoringConstants.Conf;
import storm.applications.constants.TrafficMonitoringConstants.Field;
import storm.applications.model.gis.GPSRecord;
import storm.applications.model.gis.RoadGridList;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn 
 */
public class MapMatchingBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MapMatchingBolt.class);

    private RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;

    @Override
    public void initialize() {
        String shapeFile = config.getString(Conf.MAP_MATCHER_SHAPEFILE);
        
        latMin = config.getDouble(Conf.MAP_MATCHER_LAT_MIN);
        latMax = config.getDouble(Conf.MAP_MATCHER_LAT_MAX);
        lonMin = config.getDouble(Conf.MAP_MATCHER_LON_MIN);
        lonMax = config.getDouble(Conf.MAP_MATCHER_LON_MAX);
        
        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            int speed        = input.getIntegerByField(Field.SPEED);
            int bearing      = input.getIntegerByField(Field.BEARING);
            double latitude  = input.getDoubleByField(Field.LATITUDE);
            double longitude = input.getDoubleByField(Field.LONGITUDE);
            
            if (speed <= 0) return;
            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin) return;
            
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);
            
            int roadID = sectors.fetchRoadID(record);
            
            if (roadID != -1) {
                List<Object> values = input.getValues();
                values.add(roadID);
                
                collector.emit(input, values);
            }
            
            collector.ack(input);
        } catch (SQLException ex) {
            LOG.error("Unable to fetch road ID", ex);
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.ROAD_ID);
    }
}
