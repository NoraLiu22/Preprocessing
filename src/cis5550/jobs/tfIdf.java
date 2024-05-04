package cis5550.jobs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import cis5550.kvs.KVSClient;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class tfIdf {
    static int Titleweight = 4;
    static int H1weight = 3;
    static int H2weight = 2;
    static Double threshold = 0.0;

    private static HashSet<String> stemWords(Set<String> words) {
        Stemmer s = new Stemmer();
        HashSet<String> stemmedWords = new HashSet<>();
        for (String word : words) {
            s.add(word.toCharArray(), word.length());
            s.stem();
            String stemmedWord = s.toString();
            stemmedWords.add(stemmedWord);
        }

        return stemmedWords;
    }

    public static void run(FlameContext context, String[] args) throws IOException {
        int N = context.getKVS().count("pt-crawl");
        Set<String> stopwords = StopWordsLoader.stopWords();
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        try { // compute each word's idf
              // part 1
            FlameRDD indexflameRdd = context.fromTable("pt-index", row -> {

                List<String> list = new ArrayList<>(row.columns());
                String colName = list.get(0);
                // System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhh"+colName);
                String result = row.key() + "!" + row.get(colName);
                // System.out.println("这是contextfromtable的输出"+result);
                return result;
            });
            endTime = System.currentTimeMillis();

            System.out.println("Time taken part 1: " + (endTime - startTime) + "ms");
            // part 2
            startTime = System.currentTimeMillis();

            FlamePairRDD indexflamePairRdd = indexflameRdd
                    .mapToPair(s -> new FlamePair(s.split("!")[0], s.split("!", 2)[1]));
            Map<String, Double> wordIdf = new ConcurrentHashMap<>();

            AtomicInteger i = new AtomicInteger();

            AtomicLong finalEndTime = new AtomicLong(endTime);
            AtomicLong finalStartTime = new AtomicLong(startTime);
            long startTimeFinal = startTime;
            indexflamePairRdd = indexflamePairRdd.flatMapToPair(pair -> {
                // i.getAndIncrement();
                //
                // if (i.get() % 100 == 0) {
                // finalEndTime.set(System.currentTimeMillis());
                // System.out.println("i: " + i + ", time: " + (finalEndTime.get() -
                // finalStartTime.get()) + "ms");
                // finalStartTime.set(finalEndTime.get());
                // }
                // if (i.get() == 1500) {
                // System.out.println("========TIME=========" + (System.currentTimeMillis() -
                // startTimeFinal) + "ms");
                // }

                String urlList = pair._2();
                int df = 1;
                for (int j = 0; j < urlList.length(); j++) {
                    if (urlList.charAt(j) == ',') {
                        df++;
                    }
                }

                Double idf = Math.log(1.0 * N / df);
                List<FlamePair> pairs = new ArrayList<>();

                pairs.add(new FlamePair(pair._1(), String.valueOf(idf)));
                return pairs;
            });
            endTime = System.currentTimeMillis();
            System.out.println("Time taken in part 2: " + (endTime - startTime) + "ms");

            // part 3
            startTime = System.currentTimeMillis();
            indexflamePairRdd = indexflamePairRdd.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty())
                    return u2;
                if (u2.isEmpty())
                    return u1;

                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);

                return String.valueOf(sum);
            });

            List<FlamePair> idflist = indexflamePairRdd.collect();
            Map<String, Double> idfMap = new HashMap<>();
            for (FlamePair item : idflist) {
                idfMap.put(item._1(), Double.parseDouble(item._2()));
            }
            endTime = System.currentTimeMillis();
            System.out.println("Time taken in part 3: " + (endTime - startTime) + "ms");

            // part 4
            startTime = System.currentTimeMillis();
            // compute TF
            AtomicInteger a = new AtomicInteger();
            FlameRDD flameRdd = context.fromTable("pt-crawl", row -> {
                String page = row.get("page");
                if (page != null) {
                    String result = row.get("url") + "," + row.get("page");
                    return result;
                }
                return null;
            });

            endTime = System.currentTimeMillis();
            System.out.println("Time taken in part 4: " + (endTime - startTime) + "ms, size: " + flameRdd.count()
                    + ", a: " + a.get());

            // part 5
            startTime = System.currentTimeMillis();
            FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",", 2)[1]));
            System.out.println("NEXT PRINT: " + flamePairRdd.collect().size());

            flamePairRdd = flamePairRdd.flatMapToPair(pair -> {
                String url = pair._1();
                String page = pair._2();
                if (url == null || url.equals("null") || page == null || page.equals("null")) {
                    return null;
                }

                HashSet<String> h1Words = new HashSet<>();
                HashSet<String> h2Words = new HashSet<>();
                HashSet<String> Title = new HashSet<>();

                HtmlCleaner cleaner = new HtmlCleaner();
                page = cleaner.clean(page, Title, h1Words, h2Words);

                String[] words = page.split("\\s+");

                List<String> filteredWords = Arrays.stream(words)
                        .filter(word -> !stopwords.contains(word))
                        .sorted()
                        .collect(Collectors.toList());

                words = filteredWords.toArray(new String[0]);

                Stemmer s = new Stemmer();
                List<String> Stemmedwords = new ArrayList<>();

                Set<FlamePair> pairs = new HashSet<>();

                Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
                int pos = 1;
                for (String word : words) {
                    word = word.trim().toLowerCase();

                    if (!word.isEmpty()) {
                        s.add(word.toCharArray(), word.length());
                        s.stem();
                        Stemmedwords.add(s.toString());
                        word = s.toString();
                        wordPositions.putIfAbsent(word, new TreeSet<>());
                        wordPositions.get(word).add(pos);
                        pos++;
                    }
                }

                Double DocvectorLength = 0.0;
                for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) {
                    Integer wordTf = entry.getValue().size();
                    DocvectorLength += (wordTf * wordTf);
                }
                DocvectorLength = Math.sqrt(DocvectorLength);

                Map<String, Double> normalizedTfMap = new ConcurrentHashMap<>();
                for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) {
                    String word = entry.getKey();
                    Set<Integer> positions = entry.getValue();
                    Double normalizedTf = positions.size() / DocvectorLength;
                    normalizedTfMap.put(word, normalizedTf);
                }
                // title包含的可能是原版单词没有stem过的
                Title = stemWords(Title);
                h1Words = stemWords(h1Words);
                h2Words = stemWords(h2Words);

                for (Map.Entry<String, Double> entry : normalizedTfMap.entrySet()) {
                    if (entry.getKey() == null)
                        continue;
                    if (idfMap.get(entry.getKey()) != null) {
                        Double value = (normalizedTfMap.get(entry.getKey())) * (idfMap.get(entry.getKey()));
                        if (Title.contains(entry.getKey())) {
                            value = Titleweight * value;

                        } else if (h1Words.contains(entry.getKey())) {
                            value = H1weight * value;
                        } else if (h2Words.contains(entry.getKey())) {
                            value = H2weight * value;
                        }

                        if (value >= threshold) {
                            pairs.add(new FlamePair(url + "|" + entry.getKey(), String.valueOf(value)));
                        }

                    }

                }
                return pairs;
                // return pairs;
            });
            endTime = System.currentTimeMillis();
            System.out.println(
                    "Time taken in part 5: " + (endTime - startTime) + "ms, size: " + flamePairRdd.collect().size());
            // part 6
            startTime = System.currentTimeMillis();

            flamePairRdd = flamePairRdd.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty())
                    return u2;
                if (u2.isEmpty())
                    return u1;
                // 将两个字符串转换为整数并累加
                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
                // 将累加结果转换回字符串
                return String.valueOf(sum);
            });
            endTime = System.currentTimeMillis();
            System.out.println("Time taken in part 6: " + (endTime - startTime) + "ms");
            flamePairRdd.saveAsTable("pt-tfIdf");
            ///////////////////
            startTime = System.currentTimeMillis();

            String tableName = "pt-final";
            KVSClient kvs = context.getKVS();

            List<FlamePair> pairList = flamePairRdd.collect();
            for (FlamePair pair : pairList) {
                String urlword = pair._1();
                String tfidf = pair._2();

                String word = urlword.split("\\|", 2)[1];
                String url = urlword.split("\\|", 2)[0];
                Row row = new Row(word);
                try {
                    row.put(url, tfidf);
                    kvs.putRow(tableName, row);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            endTime = System.currentTimeMillis();
            System.out.println("Time taken to write pt-final: " + (endTime - startTime) + "ms");
            ///////////////////////

            // flamePairRdd.saveAsTable("pt-tfIdf");
            // part 7
            startTime = System.currentTimeMillis();
            // 构建Table的TFIDF值，来表示Table的重要性
            FlamePairRDD flamePairRdd2 = flamePairRdd.flatMapToPair(pair -> {
                String urlword = pair._1();
                String tfidf = pair._2();
                // String word=urlword.split("?",2)[1];
                String url = urlword.split("\\|", 2)[0];
                Set<FlamePair> pairs = new HashSet<>();
                pairs.add(new FlamePair(url, tfidf));
                return pairs;
            });

            flamePairRdd2 = flamePairRdd2.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty())
                    return u2;
                if (u2.isEmpty())
                    return u1;
                // 将两个字符串转换为整数并累加
                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
                // 将累加结果转换回字符串
                return String.valueOf(sum);
            });

            flamePairRdd2.saveAsTable("pt-TableTFIDF");
            endTime = System.currentTimeMillis();
            System.out.println("Time taken in part 7: " + (endTime - startTime) + "ms");

        } catch (Exception e) {
            return;
        }

    }
}
