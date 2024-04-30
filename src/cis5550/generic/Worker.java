package cis5550.generic;
import java.net.URL;

import cis5550.tools.HTTP;
import cis5550.tools.Logger;

public class Worker {
    protected static String ipPort;
    protected static String id;
    protected static int port;
    private static final Logger logger = Logger.getLogger(Coordinator.class);

    public static void startPingThread() {
        logger.info(ipPort+" "+id+" "+port);
        startPingThread(ipPort, id, port);
    }

    public static void startPingThread(String ip_port, String id_in, int worker_port) {
        ipPort = ip_port;
        id = id_in;
        port = worker_port;
        Thread thread = new Thread(() -> {
            try {
                while (true) {
                    // logger.info("http://" + ipPort + "/ping?id=" + id + "&port=" + port);
                    HTTP.doRequest("GET", "http://"+ip_port+"/ping?id="+id+"&port="+worker_port, null);
                    // URL url = new URL("http://" + ipPort + "/ping?id=" + id + "&port=" + port);
                    Thread.sleep(5000);
                    // url.getContent();
                }
            } catch (Exception e) {
                System.out.println("Error in startPingThread: " + e);
            }

        });

        thread.start();
    }
}