package cis5550.generic;
import cis5550.tools.Logger;
import cis5550.webserver.Server;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {
    private static final Logger logger = Logger.getLogger(Coordinator.class);
    static ConcurrentHashMap<String, String[]> workerMap = new ConcurrentHashMap<>();

    public static Vector<String> getWorkers() {
        Vector<String> res = new Vector<>();

        long currentTime = System.currentTimeMillis();
        for (String id : workerMap.keySet()) {
            String[] arr = workerMap.get(id);
            if (currentTime - Long.parseLong(arr[2]) < 15000) {
                res.add(arr[0] + ":" + arr[1]);
            } else {
                logger.info(id+ " is removed...");
                workerMap.remove(id);
            }
        }
        return res;
    }

    public static String workerTable() {
        String html = "<html><table>";

        long currentTime = System.currentTimeMillis();
        for (String id : workerMap.keySet()) {
            String[] vals = workerMap.get(id);
            if (currentTime - Long.parseLong(vals[2]) < 15000) {
                html += "<tr><th>" + id + "</th><th>" + vals[0] + "</th><th>" + vals[1] + "</th><th><a href='http://" + vals[0] + ":" + vals[1] + "/'>Link</a></th></tr>";
            } else {
                logger.info(id+ " is removed...");
                workerMap.remove(id);
            }
        }

        html += "</table></html>";

        return html;
    }

    public static void registerRoutes() {
        Server.get("/ping", (req, res) -> {
            String workerId = req.queryParams("id");
            String workerPort = req.queryParams("port");
            if (workerId == null || workerPort == null) {
                res.status(500, "Bad Request");
                return null;
            }

            String ip = req.ip();

            workerMap.put(workerId, new String[]{ip, workerPort, String.valueOf(System.currentTimeMillis())});

            return "OK";
        });

        Server.get("/workers", (req, res) -> {
            long currentTime = System.currentTimeMillis();
            String body = "";

            for (String id : workerMap.keySet()) {
                String[] vals = workerMap.get(id);
                if (currentTime - Long.parseLong(vals[2]) < 15000) {
                    body += id + "," + vals[0] + ":" + vals[1] + "\n";
                } else {
                    logger.info(id+ " is removed...");
                    workerMap.remove(id);
                }
            }

            return String.valueOf(workerMap.size()) + "\n" + body;
        });

        Server.get("/", (req, res) -> {
            String html = "<html>" +
                    "<head><title>KVS Coordinator</title></head>" +
                    "<body><div>" + workerTable() + "</div></body></html>";
            return html;
        });
    }
};