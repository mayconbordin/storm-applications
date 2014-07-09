package storm.applications.util.ip;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: domenicosolazzo
 */
public class HttpIPResolver implements IPResolver, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(HttpIPResolver.class);

    static String url = "http://api.hostip.info/get_json.php";

    @Override
    public JSONObject resolveIP(String ip) {
        URL geoUrl = null;
        BufferedReader in = null;
        try {
            geoUrl = new URL(url + "?ip=" + ip);
            URLConnection connection = geoUrl.openConnection();
            in = new BufferedReader(new InputStreamReader(
                connection.getInputStream()
            ));

            JSONObject json = (JSONObject) JSONValue.parse(in);
            in.close();

            return json;
        } catch (MalformedURLException ex) {
            LOG.error("URL is not valid", ex);
        } catch (IOException ex) {
            LOG.error("Error while resolving IP location", ex);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch(IOException ex) {
                    LOG.error("Error while resolving IP location", ex);
                }
            }
        }
        return null;
    }
}
