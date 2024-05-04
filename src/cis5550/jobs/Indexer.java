package cis5550.jobs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;
import cis5550.flame.FlameContext;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
//import cis5550.tools.URLParser;
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

public class Indexer 
{
    
   //RowToString 
    public static void run(FlameContext context,String[] args)
    {    

        Set<String> stopwords=StopWordsLoader.stopWords();
        Set<String> wordSet=DictionaryFilter.loader();
        try
        {
            
            FlameRDD flameRdd=context.fromTable("pt-crawl", row -> 
            {   
                String page = row.get("page");

                
                if(page!=null) 
                {
                    String result = row.get("url") + "," + row.get("page");
                    
                    return result;
                }
                return null;
            });
        //FlamePairRDD mapToPair(StringToPair lambda)
        //s 是来自于 flameRdd 中的每一个元素，即 fromTable 方法中每一行经过处理后得到的结果
        
            FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
            flamePairRdd=flamePairRdd.flatMapToPair( pair -> 
            {
                String url = pair._1();
		        String page = pair._2();
                if(url==null || url.equals("null") || page==null || page.equals("null"))
                {
                    return null;
                }
                HtmlCleaner cleaner=new HtmlCleaner();
                page=cleaner.clean2(page);
                // Split into words
		        String[] words = page.split("\\s+");
                List<String> filteredWords = Arrays.stream(words)
                .filter(word -> !stopwords.contains(word) && wordSet.contains(word))
                .sorted() // 对过滤好的单词进行排序
                .collect(Collectors.toList());
                // 将排序后的单词重新存储在String[] words中
                words = filteredWords.toArray(new String[0]);

                //System.out.println("filtered Num of Words: " + words.length);
                // do stemming
                Stemmer s = new Stemmer();
                List<String> Stemmedwords= new ArrayList<>();;
                Set<FlamePair> pairs = new HashSet<>();
                //是一个文档中的每个词出现位置的对照表
                Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
                int pos = 1;
                for(String word: words)
                {   
                    word = word.trim();
                    word = word.toLowerCase();
                    if(word == null || word.isEmpty()) 
                    {continue;}
                    if(!word.isEmpty()) 
                    {   
                        s.add(word.toCharArray(), word.length());
                        s.stem();
                        Stemmedwords.add(s.toString());
                        word=s.toString();
                        wordPositions.putIfAbsent(word,new TreeSet<>());
                        wordPositions.get(word).add(pos);
                        pos++;
                    }
                }

                Collections.sort(Stemmedwords);
                words = Stemmedwords.toArray(new String[0]);

                for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) 
                {
                    String word = entry.getKey();
                    Set<Integer> positions = entry.getValue();
                    if (word == null || word.isEmpty() || positions.isEmpty()) 
                    {continue;} // 跳过无效或空数据
                    // 将positions转换为排序后的空格分隔字符串
                    StringBuilder positionsStrBuilder = new StringBuilder();
                    for (Integer position : positions) 
                    {
                        // 仅在非第一个元素前添加空格
                        if (positionsStrBuilder.length() > 0) 
                        {
                            positionsStrBuilder.append(" ");
                        }
                        positionsStrBuilder.append(position);
                    }
                    //int size=positions.size();
                    String positionsStr = positionsStrBuilder.toString();
                    FlamePair flamePair =new FlamePair(word, url + ":" + positionsStr);
                    pairs.add(flamePair);
                }
                return pairs; 
                //return pairs; 
            });


            //public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception
            flamePairRdd=flamePairRdd.foldByKey("", (u1, u2) -> 
            {
                if (u1.isEmpty()) {
			        return u2;
			    } else if (u2.isEmpty()) {
			        return u1;
			    } else 
                {
                    List<String> urlList = new ArrayList<>(Arrays.asList((u1 + "," + u2).split(",")));

			        return String.join(",", urlList);
			    }

            });
            //// flatMapToPair() is analogous to flatMap(), except that the lambda returns pairs 
  // instead of strings, and tha tthe output is a PairRDD instead of a normal RDD.
//public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception;
            flamePairRdd=flamePairRdd.flatMapToPair( pair -> 
            {
                String word = pair._1();
		        String urlList = pair._2();
                String[] urls = urlList.trim().split(",");
                Map<String, Integer> urlCounts = new HashMap<>();
                // bar.com:2 5 12,foo.com:3 8
                for (String url : urls) 
                {
                    String[] parts = url.split(":");
                    if(parts.length < 2|| parts[1].trim().isEmpty() ) 
                    {continue;}
                    // 计算每个URL后面跟随的数字数量
                    urlCounts.put(url, parts[1].trim().split(" ").length);
                }
                // 根据数字数量对URL进行排序
                List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(urlCounts.entrySet());
                sortedEntries.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));
                // 组装最终的字符串
                List<String> sortedUrls = new ArrayList<>();
                for (Map.Entry<String, Integer> entry : sortedEntries) 
                {
                    sortedUrls.add(entry.getKey());
                }

                String sortedUrlList=String.join(",", sortedUrls);
                Set<FlamePair> pairs = new HashSet<>();
                FlamePair flamePair =new FlamePair(word, sortedUrlList);
                pairs.add(flamePair);
                return pairs;
            });
            
            flamePairRdd.saveAsTable("pt-index");

            
        }catch(Exception e)
        {

        }
    
    }

}
