/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.storm.applications.sink;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SocketSink extends BaseSink {
    private int port = 6000;
    private List<Socket> connections;
    private List<OutputStreamWriter> outputStreams;
    private Thread connectionListener;
    
    public SocketSink(){}
    public SocketSink(int port) {
        this.port = port;
    }
    
    private class ConnectionListener implements Runnable {
        @Override
        public void run() {
            try {
                ServerSocket socket = new ServerSocket(port);
                
                while (true) {
                    Socket connection = socket.accept();
                    connections.add(connection);
                    
                    BufferedOutputStream os = new BufferedOutputStream(connection.getOutputStream());
                    OutputStreamWriter osw = new OutputStreamWriter(os, "US-ASCII");
                    
                    outputStreams.add(osw);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        connections = new ArrayList<Socket>();
        outputStreams = new ArrayList<OutputStreamWriter>();
        
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
            ex.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String message = tuple.getString(0) + "\t" + tuple.getInteger(1).toString() + "\n";
            
            for (OutputStreamWriter osw : outputStreams) {
                osw.write(message);
                osw.flush();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
}
