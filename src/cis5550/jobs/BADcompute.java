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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BADcompute 
{
    
public static void run(FlameContext context,String[] args)
{    
 try
 {  
    //compute each word's idf
    int N =10;
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
    //System.out.println("zzzzzzzzzzzzzzzzzzzzzz1111111111111111111");
    indexflamePairRdd=indexflamePairRdd .flatMapToPair( pair -> 
    {   
        String word = pair._1();
        String urlList = pair._2();
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
                // String.valueOf(normalizedTfMap.get(entry.getKey()))
                //String.valueOf(entry.getValue())
            //System.out.println("kkkkkkkkkkkkk111111111111");
            if(wordIdf.get(entry.getKey())!=null)
            {    //System.out.println("kkkkkkkkkkkkk22222222222222");
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
    indexflamePairRdd.saveAsTable("pt-idf");

    //构建tfidf
 FlameRDD tfidfflameRdd=context.fromTable("pt-tf", row -> 
 {   
     List<String> list = new ArrayList<>(row.columns());
      String colName=list.get(0);
     //System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhh"+colName);
     System.out.println("hhhhhhhhhhhh2222222222222222"+colName);
     String result=row.key()+"!"+row.get(colName);
     System.out.println("这是contextfromtable的输出22222222222"+result);
    return result;
 });
FlamePairRDD tfidfflamepairRdd = tfidfflameRdd.mapToPair(s -> new FlamePair(s.split("!")[0], s.split("!",2)[1]));
System.out.println("hhhhhhhhhhhh33333333333333");
List<FlamePair> idflist = indexflamePairRdd.collect();
Map<String, String> idfMap = new HashMap<>();

for (FlamePair item : idflist) 
{
  idfMap.put(item._1(), item._2());
}
int idfsize =idfMap.size();
System.out.println("IDFMap 的长度为: " + idfsize);

tfidfflamepairRdd=tfidfflamepairRdd.flatMapToPair( pair -> 
 {
     String urlword = pair._1();
    String tf = pair._2();
    String word=urlword.split(":",2)[1];
    String tfidf=null;
    Set<FlamePair> pairs = new HashSet<>();
     // 直接从 HashMap 中获取 IDF 值
     if (idfMap.containsKey(word)) 
     {
         String idf = idfMap.get(word);
         tfidf = String.valueOf(Double.parseDouble(tf) * Double.parseDouble(idf));
         //tfidf+":"+tf+":"+idf
         pairs.add(new FlamePair(urlword,tfidf));
     }
     return pairs;
 }); 
 tfidfflamepairRdd=tfidfflamepairRdd.foldByKey("0.0", (u1, u2) -> {
     if (u1.isEmpty()) return u2;
     if (u2.isEmpty()) return u1;
     // 将两个字符串转换为整数并累加
     Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
     // 将累加结果转换回字符串
     return String.valueOf(sum);
 });
 tfidfflamepairRdd.saveAsTable("pt-tfIdf");
}
catch(Exception e)
{
}
}
}
