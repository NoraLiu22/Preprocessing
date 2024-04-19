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
import cis5550.tools1.Hasher;
import cis5550.external.PorterStemmer;
import java.util.TreeSet;
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

public class tfIdf 
{
    static int Titleweight=4;
    static int H1weight=3;
    static int H2weight=2;
    static Double threshold=1.0;
    
    private static HashSet<String> stemWords(Set<String> words) 
    {
        Stemmer s = new Stemmer();
        HashSet<String> stemmedWords = new HashSet<>();
        for (String word : words) 
        {
            s.add(word.toCharArray(), word.length());
            s.stem();        
            String stemmedWord=s.toString();
            stemmedWords.add(stemmedWord);
        }

        return stemmedWords;
    }
    
    public static void run(FlameContext context,String[] args)
    {    
       int N =9;
        Set<String> stopwords=StopWordsLoader.stopWords();
        try
         {   //compute each word's idf
            FlameRDD indexflameRdd=context.fromTable("pt-index", row -> 
            {   
                
                List<String> list = new ArrayList<>(row.columns());
                String colName=list.get(0);
                //System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhh"+colName);
                String result=row.key()+"!"+row.get(colName);
                //System.out.println("这是contextfromtable的输出"+result);
               return result;
            });
            FlamePairRDD indexflamePairRdd = indexflameRdd.mapToPair(s -> new FlamePair(s.split("!")[0], s.split("!",2)[1]));
            Map<String, Double> wordIdf = new ConcurrentHashMap<>();
        //     //System.out.println("zzzzzzzzzzzzzzzzzzzzzz1111111111111111111");
            indexflamePairRdd=indexflamePairRdd .flatMapToPair( pair -> 
            {   
                String word = pair._1();
                //System.out.println("这是word: "+word);
		        String urlList = pair._2();
                //System.out.println("这是urlList: "+urlList);
                Integer df = urlList.split(",").length + 1;//avoid divide by 0
                //System.out.println("这是DF: "+df);
	            Double idf = Math.log(1.0 * N / df);
                //System.out.println("这是IDF: "+idf);
                wordIdf.put(word,idf);
                Set<FlamePair> pairs = new HashSet<>();
                for (Map.Entry<String, Double> entry : wordIdf.entrySet()) 
                {
	            	//put tf + normalizedTf
	            	if(entry.getKey()==null)
	            		continue;
                        
                    if(wordIdf.get(entry.getKey())!=null)
                    {    
                        pairs.add(new FlamePair(entry.getKey(), String.valueOf((wordIdf.get(entry.getKey())))));
                    }   
	            }
                return pairs;
            });
            indexflamePairRdd = indexflamePairRdd.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty()) return u2;
                if (u2.isEmpty()) return u1;
                // 将两个字符串转换为整数并累加
                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
                // 将累加结果转换回字符串
                return String.valueOf(sum);
            });
            List<FlamePair> idflist = indexflamePairRdd.collect();
            //indexflamePairRdd.saveAsTable("pt-idf");
            Map<String, Double> idfMap = new HashMap<>();
            for (FlamePair item : idflist) 
            {
            idfMap.put(item._1(),  Double.parseDouble(item._2()));
            }

            //compute TF
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
            //public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception;
           
            flamePairRdd=flamePairRdd.flatMapToPair( pair -> 
            {
                String url = pair._1();
		        String page = pair._2();
                if(url==null || url.equals("null") || page==null || page.equals("null"))
                {
                    return null;
                }
                   
                    HashSet<String> h1Words = new HashSet<>();
                    HashSet<String> h2Words = new HashSet<>();
                    HashSet<String> Title = new HashSet<>();

                HtmlCleaner cleaner=new HtmlCleaner();
                page=cleaner.clean(page,Title,h1Words,h2Words);
                // // Remove content from meta, script and link tags
		        // String patternString = "<(meta|script|link)(\\s[^>]*)?>.*?</(meta|script|link)>";
		        // // Compile the pattern
		        // Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		        // // Match the pattern against the HTML string
		        //  Matcher matcher = pattern.matcher(page);
		        //  page = matcher.replaceAll(" ");
		        //  // Remove HTML tags
		        // page = page.replaceAll("<.*?>", " ");
		        // // Remove punctuation
		        // page = page.replaceAll("[.,:;!?'\"\\(\\)-]", " ");   
		        // //Remove non alpha numeric characters
                // //[^a-zA-Z0-9]
		        // page = page.replaceAll("[^a-zA-Z]", " ");
		        // //Remove non ASCII characters
		        // page = page.replaceAll("[^\\p{ASCII}]", " ");
                // page = page.replaceAll("[\\r\\n\\t]", " ");
                // page=page.toLowerCase();
                //System.out.println(page);
                // Split into words
		        String[] words = page.split("\\s+");
                // drop stop words
                //System.out.println("Original Num of Words: " + words.length);
                //int stopwordsCount = stopwords.size();
                //System.out.println("Num of StopWords: " + stopwordsCount);
                List<String> filteredWords = Arrays.stream(words)
                .filter(word -> !stopwords.contains(word))
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
                //compute L2 norm over all document level term frequencies
	            Double DocvectorLength = 0.0;
	        	for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) 
                {
	        		 Integer wordTf = entry.getValue().size();
	        		 DocvectorLength+=(wordTf*wordTf);
                    
	        	}
	        	DocvectorLength = Math.sqrt(DocvectorLength);//compute each doc's vector length
	            // Compute term frequency (tf) and normalizedTf

				Map<String, Integer> tfMap = new ConcurrentHashMap<>();
				Map<String, Double> normalizedTfMap = new ConcurrentHashMap<>();
				for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) 
                {
					String word = entry.getKey();
					Set<Integer> positions = entry.getValue();
					Double normalizedTf = positions.size() / DocvectorLength;
					tfMap.put(word,positions.size());
					normalizedTfMap.put(word, normalizedTf);
				}
                //title包含的可能是原版单词没有stem过的
                Title=stemWords(Title);
                h1Words=stemWords(h1Words);
                h2Words=stemWords(h2Words);

	            for (Map.Entry<String, Double> entry : normalizedTfMap.entrySet()) 
                {
	            	//put tf + normalizedTf
	            	if(entry.getKey()==null)
	            		continue;
                    if(idfMap.get(entry.getKey())!=null)
                    {    
                        Double value=(normalizedTfMap.get(entry.getKey()))*(idfMap.get(entry.getKey()));
                        if (Title.contains(entry.getKey())) 
                        {
                            //System.out.println("加了title权重");
                            //System.out.println("加了title权重之前: "+value);
                            value=Titleweight*value;
                            //System.out.println("加了title权重之后: "+value);

                        }
                        else if(h1Words.contains(entry.getKey()))
                        {
                            //System.out.println("加了Header1权重");
                            //System.out.println("加了Header1权重之前: "+value);
                            value=H1weight*value;
                            //System.out.println("加了Header1权重之后: "+value);
                        }
                        else if(h2Words.contains(entry.getKey()))
                        {   
                            //System.out.println("加了Header2权重");
                            value=H2weight*value;
                        }
                        //String.valueOf((normalizedTfMap.get(entry.getKey()))*(idfMap.get(entry.getKey())))
                        if(value>=threshold)
                        {
                            pairs.add(new FlamePair(url + "?" + entry.getKey(),String.valueOf(value)));
                        }
                        
                    }
                    
	            }
                return pairs; 
                //return pairs; 
            });

            flamePairRdd=flamePairRdd.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty()) return u2;
                if (u2.isEmpty()) return u1;
                // 将两个字符串转换为整数并累加
                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
                // 将累加结果转换回字符串
                return String.valueOf(sum);
            });

            ///////////////////

            String tableName = "pt-final";
            KVSClient kvs = context.getKVS();

            List<FlamePair> pairList = flamePairRdd.collect();
            for (FlamePair pair : pairList) {
                String urlword = pair._1();
                String tfidf = pair._2();

                String word = urlword.split("\\?", 2)[1];
                String url = urlword.split("\\?", 2)[0];
                Row row = new Row(word);
                row.put(url, tfidf);
                kvs.putRow(tableName, row);
            }

            ///////////////////////


            //flamePairRdd.saveAsTable("pt-tfIdf");

            //构建Table的TFIDF值，来表示Table的重要性
            FlamePairRDD flamePairRdd2=flamePairRdd.flatMapToPair( pair -> 
            {
                String urlword = pair._1();
		        String tfidf = pair._2();
                //String word=urlword.split("?",2)[1];
                String url=urlword.split("\\?",2)[0];
                Set<FlamePair> pairs = new HashSet<>();
                pairs.add(new FlamePair(url ,tfidf ));
                return pairs;
            });
            
            flamePairRdd2=flamePairRdd2.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty()) return u2;
                if (u2.isEmpty()) return u1;
                // 将两个字符串转换为整数并累加
                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
                // 将累加结果转换回字符串
                return String.valueOf(sum);
            });

            flamePairRdd.saveAsTable("pt-tfIdf");
            flamePairRdd2.saveAsTable("pt-TableTFIDF");

            //System.out.println("hhhhhhhhhhhh1111111111");

        // //构建tfidf
        // FlameRDD tfidfflameRdd=context.fromTable("pt-tf", row -> 
        //     {   
        //         // List<String> list = new ArrayList<>(row.columns());
        //         // String colName=list.get(0);
        //         //System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhh"+colName);
        //         System.out.println("hhhhhhhhhhhh2222222222222222");
        //         String result=row.key()+"!"+row.get("acc");
        //         System.out.println("这是contextfromtable的输出22222222222"+result);
        //        return result;
        //     });
        //  FlamePairRDD tfidfflamepairRdd = tfidfflameRdd.mapToPair(s -> new FlamePair(s.split("!")[0], s.split("!",2)[1]));
        //  System.out.println("hhhhhhhhhhhh33333333333333");
        //  List<FlamePair> idflist = indexflamePairRdd.collect();
        //  Map<String, String> idfMap = new HashMap<>();

        //  for (FlamePair item : idflist) 
        //  {
        //      idfMap.put(item._1(), item._2());
        //  }
        //  int idfsize =idfMap.size();
        // System.out.println("IDFMap 的长度为: " + idfsize);
        
        //   tfidfflamepairRdd=tfidfflamepairRdd.flatMapToPair( pair -> 
        //     {
        //         String urlword = pair._1();
		//        String tf = pair._2();
        //        String word=urlword.split(":",2)[1];
        //        String tfidf=null;
        //        Set<FlamePair> pairs = new HashSet<>();
        //         // 直接从 HashMap 中获取 IDF 值
        //         if (idfMap.containsKey(word)) 
        //         {
        //             String idf = idfMap.get(word);
        //             tfidf = String.valueOf(Double.parseDouble(tf) * Double.parseDouble(idf));
        //             //tfidf+":"+tf+":"+idf
        //             pairs.add(new FlamePair(urlword,tfidf));
        //         }
        //         return pairs;
        //     }); 
        //     tfidfflamepairRdd=tfidfflamepairRdd.foldByKey("0.0", (u1, u2) -> {
        //         if (u1.isEmpty()) return u2;
        //         if (u2.isEmpty()) return u1;
        //         // 将两个字符串转换为整数并累加
        //         Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
        //         // 将累加结果转换回字符串
        //         return String.valueOf(sum);
        //     });
        //     tfidfflamepairRdd.saveAsTable("pt-tfIdf");



        //     FlameRDD xflameRdd=context.fromTable("pt-idf", row -> 
        //     {   
                
        //         List<String> list = new ArrayList<>(row.columns());
        //         String colName=list.get(0);
        //         //System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhh"+colName);
        //         String result=row.key()+"!"+row.get(colName);
        //         //System.out.println("这是contextfromtable的输出"+result);
        //        return result;
        //     });
        // FlamePairRDD xflamepairRdd = xflameRdd.mapToPair(s -> new FlamePair(s.split("!")[0], s.split("!",2)[1]));
        // xflamepairRdd.flatMapToPair( pair2 -> 
        // {
        //     String word2=pair2._1();
        //    String idf=pair2._2();
        //    if(word==word2)
        //    {
        //     tf=String.valueOf(Double.parseDouble(pair._2()) *Double.parseDouble(idf));
        //    }
        //    Set<FlamePair> pairs2 = new HashSet<>();
        //    return pairs2;
        // }); 
        // Set<FlamePair> pairs = new HashSet<>();
        // pairs.add(new FlamePair(urlword,tf));
        // return pairs;
        // });   
        
        }catch(Exception e)
        {
        }
        
    
    }
}

