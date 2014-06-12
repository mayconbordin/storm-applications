package org.storm.applications.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.storm.applications.util.BingMapsLookup;
import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;

/**
 * Gets the location of tweet by all 3 means and then fwds the State code with the tweet to the next Bolt.
 * There are three different objects within a tweet that we can use to determine it’s origin.
 * This Class utilizes all the three of them and prioritizes in the following order [high to low]:
 *  1. The coordinates object
 *  2. The place object
 *  3. The user object
 * 
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public final class LocateStateBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LocateStateBolt.class);
    private static final long serialVersionUID = -8097813984907419942L;
    private static final List<String> CONSOLIDATED_STATE_CODES = Lists.newArrayList("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","MD","MA","MI","MN","MS","MO","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY");
    
    private OutputCollector _outputCollector;
    private String bingMapsApiKey;

    public LocateStateBolt() {
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
            final OutputCollector outputCollector) {
        this._outputCollector = outputCollector;
        bingMapsApiKey = (String) map.get("BING_MAPS_API_KEY");
        BingMapsLookup.setApiKey(bingMapsApiKey);
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        //Emit the state and also the complete tweet to the next Bolt.
        outputFieldsDeclarer.declare(new Fields("state", "tweet"));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void execute(final Tuple input) {
        final Status status = (Status) input.getValueByField("tweet");
        final Optional<String> stateOptional = getStateFromTweet(status);
        if(stateOptional.isPresent()) {
            final String state = stateOptional.get();
            //Emit the state and also the complete tweet to the next Bolt.
            this._outputCollector.emit(new Values(state, status));
        }
    }

    /**
     * Tries to get the State of the tweet by checking first GeoLocation Object, then Place Object and finally User Object.
     *
     * @param status -- Status Object.
     * @return State of the Tweet.
     */
    private Optional<String> getStateFromTweet(final Status status) {
        String state = getStateFromTweetGeoLocation(status);

        state = getStateFromTweetPlaceObject(status, state);

        state = getStateFromTweetUserObject(status, state);

        if (null == state || !CONSOLIDATED_STATE_CODES.contains(state)) {
            LOG.info("Skipping invalid State: {}.", state);
            return Optional.absent();
        }
        LOG.debug("State:{}", state);
        return Optional.of(state);
    }

    /**
     * Retrieves the State from User Object of the Tweet.
     *
     * @param status -- Status Object.
     * @param state -- Current State.
     * @return State of tweet.
     */
    private String getStateFromTweetUserObject(final Status status, String state) {
        String stateFromUserObject = status.getUser().getLocation();
        if(null == state && null != stateFromUserObject && 1 < stateFromUserObject.length()) {
            String stateUser = stateFromUserObject.substring(stateFromUserObject.length() - 2).toUpperCase();
            LOG.debug("State from User:{}", stateFromUserObject);
            //Retry to get the State of the User if the last 2 chars are US for the User's Location object.
            //This is just a pro-active check.
            //This assumes the format: NY, US
            if("US".equalsIgnoreCase(stateUser) && 5 < stateFromUserObject.length()){
                stateUser = stateFromUserObject.substring(stateFromUserObject.length() - 6, stateFromUserObject.length() - 4);
                LOG.debug("State from User again:{}", stateFromUserObject);
            }
            state = (2 == stateUser.length())? stateUser.toUpperCase(): null;
        }
        return state;
    }

    /**
     * Retrieves the State from Place Object of the Tweet.
     *
     * @param status -- Status Object.
     * @param state -- Current State.
     * @return State of tweet.
     */
    private String getStateFromTweetPlaceObject(final Status status, String state) {
        final Place place = status.getPlace();
        if (null == state && null != place) {
            final String placeName = place.getFullName();
            if (null != placeName && 2 < placeName.length()) {
                final String stateFromPlaceObject = placeName.substring(placeName.length() - 2);
                LOG.debug("State from Place:{}", stateFromPlaceObject);
                state = (2 == stateFromPlaceObject.length())? stateFromPlaceObject.toUpperCase(): null;
            }
        }
        return state;
    }

    /**
     * Retrieves the State from GeoLocation Object of the Tweet.
     * This is considered as the primary and correct value for the State of the tweet.
     *
     * @param status -- Status Object.
     * @return State of tweet.
     */
    private String getStateFromTweetGeoLocation(final Status status) {
        String state = null;
        final double latitude;
        final double longitude;
        final GeoLocation geoLocation = status.getGeoLocation();
        if (null != geoLocation) {
            latitude = geoLocation.getLatitude();
            longitude = geoLocation.getLongitude();
            LOG.debug("LatLng for BingMaps:{} and {}", latitude, longitude);
            final Optional<String> stateGeoOptional = BingMapsLookup.reverseGeocodeFromLatLong(latitude, longitude);
            if(stateGeoOptional.isPresent()){
                final String stateFromGeoLocation = stateGeoOptional.get();
                LOG.debug("State from BingMaps:{}", stateFromGeoLocation);
                state = (2 == stateFromGeoLocation.length())? stateFromGeoLocation.toUpperCase(): null;
            }
        }
        return state;
    }
}