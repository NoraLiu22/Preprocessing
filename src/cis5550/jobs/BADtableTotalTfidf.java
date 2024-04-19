package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import java.util.HashSet;
import java.util.Set;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class BADtableTotalTfidf 
{
    public static void run(FlameContext context,String[] args)
    {    
        try
        {
            System.out.println("111111111111111111");
            String masterAddr = context.getKVS().getCoordinator();
            KVSClient kvs = new KVSClient(masterAddr);
            System.out.println("pt-tfIdf表的行数: "+kvs.count("pt-tfIdf"));

            FlameRDD flameRdd=context.fromTable("pt-tfIdf", row -> 
            {   
                System.out.println("2222222222222222222");
                String result=row.get("Key")+"!"+row.get("acc");  
                //String result= row.get("acc");
                System.out.println("33333333333333333333");
               return result;
            });

            System.out.println("44444444444444444444");
            FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split("!")[0], s.split("!",2)[1]));
            System.out.println("555555555555555555555");
            flamePairRdd=flamePairRdd.flatMapToPair( pair -> 
            {
                String urlword = pair._1();
		        String tfidf = pair._2();
                //String word=urlword.split("?",2)[1];
                String url=urlword.split("\\?",2)[0];
                Set<FlamePair> pairs = new HashSet<>();
                pairs.add(new FlamePair(url ,tfidf ));
                return pairs;
            });
            System.out.println("6666666666666666666666666666655");
            flamePairRdd=flamePairRdd.foldByKey("0.0", (u1, u2) -> {
                if (u1.isEmpty()) return u2;
                if (u2.isEmpty()) return u1;
                // 将两个字符串转换为整数并累加
                Double sum = Double.parseDouble(u1) + Double.parseDouble(u2);
                // 将累加结果转换回字符串
                return String.valueOf(sum);
            });
            System.out.println("7777777777777777777777");
            flamePairRdd.saveAsTable("pt-TableTFIDF");

        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
