package cis5550.jobs;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class test {
    public static void main(String[] args) {
        // join all the words in the query
        String query = String.join(" ", args);
        try {
            // String urlString = "http://localhost:9001/rank/";
            // System.out.println("urlString: " + urlString);
            URL url = new URL("http://localhost:9001/rank"); // Adjust the URL path
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            // String data = "{ \"key\": \"value\" }"; // JSON payload you want to send

            // Sending the request
            OutputStream os = conn.getOutputStream();
            os.write(query.getBytes());
            os.flush();
            os.close();

            // Handling the response
            int responseCode = conn.getResponseCode();
            System.out.println("POST Response Code : " + responseCode);
            System.out.println("POST Response Message : " + conn.getResponseMessage());

            if (responseCode == HttpURLConnection.HTTP_OK) { // success
                // Further processing here
            } else {
                System.out.println("POST request not worked");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
