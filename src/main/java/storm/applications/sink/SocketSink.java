package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SocketSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(SocketSink.class);
    
    private int port;
    private String charset;

    private List<Socket> connections;
    private List<OutputStreamWriter> outputStreams;
    private Thread connectionListener;
    
    @Override
    public void initialize() {
        super.initialize();
        
        port = config.getInt(getConfigKey(BaseConf.SINK_SOCKET_PORT), 6000);
        charset = config.getString(getConfigKey(BaseConf.SINK_SOCKET_CHARSET), "US-ASCII");
        
        connections   = new ArrayList<>();
        outputStreams = new ArrayList<>();
        
        Runnable runnable = new ConnectionListener();
        connectionListener = new Thread(runnable);
        connectionListener.start();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        
        try {
            connectionListener.interrupt();

            for (Socket s : connections) {
                s.close();
            }
        } catch (IOException ex) {
            LOG.error("Unable to close socket connections", ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String message = formatter.format(tuple);
            
            for (OutputStreamWriter osw : outputStreams) {
                osw.write(message);
                osw.flush();
            }
            
            collector.ack(tuple);
        } catch (IOException ex) {
            LOG.error("Unable to send message", ex);
        }
    }
    
    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
    private class ConnectionListener implements Runnable {
        @Override
        public void run() {
            try {
                ServerSocket socket = new ServerSocket(port);
                
                while (true) {
                    Socket connection = socket.accept();
                    LOG.info("New connection established with " + connection.getInetAddress());
                    connections.add(connection);
                    
                    BufferedOutputStream os = new BufferedOutputStream(connection.getOutputStream());
                    OutputStreamWriter osw  = new OutputStreamWriter(os, charset);
                    
                    outputStreams.add(osw);
                }
            } catch (IOException ex) {
                LOG.error("Connection error", ex);
            }
        }
    }
}
