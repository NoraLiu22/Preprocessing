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

public class Ranker {

    private static String stemmedWord(String word) {
        word = word.toLowerCase();
        Stemmer s = new Stemmer();
        s.add(word.toCharArray(), word.length());
        s.stem();
        return s.toString();
    }

    public static void run(FlameContext context, String[] args) {
        int N = 10;
        String query = "hello world";
        Set<String> stopwords = StopWordsLoader.stopWords();
        String[] keyWords = query.split(" ");

        List<String> filteredWords = Arrays.stream(keyWords)
                .map(word -> stemmedWord(word))
                .filter(word -> !stopwords.contains(word))
                .collect(Collectors.toList());

        // String[] keyWords = { "attack" };
        KVSClient kvs = context.getKVS();
        String tableName = "pt-final";
        Map<String, Double> urlValue = new HashMap<String, Double>();
        try { // compute each word's idf
            for (String kw : filteredWords) {
                Row row = kvs.getRow(tableName, kw);
                for (String url : row.columns()) {
                    String value = row.get(url);
                    if (urlValue.containsKey(url)) {
                        urlValue.put(url, urlValue.get(url) + Double.parseDouble(value));
                    } else {
                        urlValue.put(url, Double.parseDouble(value));
                    }
                }
            }

            // sort the urls by value
            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(urlValue.entrySet());
            Collections.sort(list, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

            // show the top urls
            int size = Math.min(N, list.size());
            for (int i = 0; i < size; i++) {
                System.out.println(list.get(i).getKey() + " " + list.get(i).getValue());
            }

        } catch (Exception e) {
        }

    }
}
