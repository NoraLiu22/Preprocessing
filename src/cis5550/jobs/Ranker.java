package cis5550.jobs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;
import cis5550.flame.FlameContext;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools1.Hasher;
import cis5550.external.PorterStemmer;
import java.util.TreeSet;
import java.util.stream.Collectors;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cis5550.webserver.Server.*;
import cis5550.webserver.Request;

class Ranker extends cis5550.generic.Worker {

    private static String stemmedWord(String word) {
        word = word.toLowerCase();
        Stemmer s = new Stemmer();
        s.add(word.toCharArray(), word.length());
        s.stem();
        return s.toString();
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);
        // final File myJAR = new File("__worker"+port+"-current.jar");

        port(port);
        String coordinatorArg = "localhost:8000";
        KVSClient kvs = new KVSClient(coordinatorArg);

        post("/rank", (req, res) -> {
            String query = req.body();

            System.out.println("Query: " + query);
            // number of urls to return
            int N = 10;
            // query = "hello world";
            Set<String> stopwords = StopWordsLoader.stopWords();

            String[] keyWords = query.split(" ");
            // for (String word : keyWords) {
            // System.out.println(word);
            // System.out.println(stemmedWord(word));
            // }

            List<String> filteredWords = Arrays.stream(keyWords)
                    .map(word -> stemmedWord(word))
                    // .filter(word -> !stopwords.contains(word))
                    .collect(Collectors.toList());

            // output filteredWords
            for (String word : filteredWords) {
                System.out.println(word);
            }
            // String[] keyWords = { "attack" };
            String tableName = "pt-final";
            Map<String, Double> urlValue = new HashMap<String, Double>();
            try { // compute each word's idf
                for (String kw : filteredWords) {
                    Row row = kvs.getRow(tableName, kw);
                    if (row == null) {
                        continue;
                    }
                    for (String url : row.columns()) {
                        String value = row.get(url);
                        if (urlValue.containsKey(url)) {
                            // System.out.println(url);
                            urlValue.put(url, urlValue.get(url) + Double.parseDouble(value));
                        } else {
                            // System.out.println(url);
                            urlValue.put(url, Double.parseDouble(value));
                        }
                    }
                }

                // sort the urls by value
                List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(urlValue.entrySet());
                Collections.sort(list, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

                // show the top urls
                int size = Math.min(N, list.size());
                System.out.println("Top " + size + " urls:");
                for (int i = 0; i < size; i++) {
                    System.out.println(list.get(i).getKey() + " " + list.get(i).getValue());
                }

                // pass the top urls to the front end

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

    }
}
