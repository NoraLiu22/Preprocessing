package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools1.Hasher;

import java.util.Iterator;
import java.util.Collections;

public class PageRank 
{
     public static void run(FlameContext context,String[] args)
    {
        String masterAddr = context.getKVS().getCoordinator();
    try{
        // public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception;
        //make (u,"1.0,1.0,L") pairs, where L is a comma-separated list of normalized links you found on the page with URL hash u
        //List<String> extract(String base, String page) 
        

        Double convergenceThreshold = args.length >= 1 ? Double.parseDouble(args[0]) : 0.01;
        Double convergencePercentage = args.length >= 2 ? Double.parseDouble(args[1]) : 100.0;

        System.out.println("convergenceThreshold: " + convergenceThreshold + ", convergencePercentage: " + convergencePercentage);
	
        FlameRDD flameRdd=context.fromTable("pt-crawl", row -> 
            {
                String page = row.get("page");
                String url=row.get("url");
                String hashurl=Hasher.hash(url);
                if (page == null) 
                {
					return null;
				}
                else
                {
                    List<String>urls= tools.extract(url, page) ;
                    String ranks = "1.0,1.0"; // 初始的当前和前一个排名值
                    String L = String.join(",", urls); // 将标准化的链接列表用逗号连接起来
                    String result = hashurl + ";" + ranks + "," + L;
                    return result;
                }
                
            });
            FlamePairRDD stateTable = flameRdd.mapToPair(s -> new FlamePair(s.split(";")[0], s.split(";",2)[1]));
            //public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception
            int numberOfIterations = 0;
			while(true) 
            {
                numberOfIterations++;
                System.out.println("numberOfIterations: " + numberOfIterations);

                FlamePairRDD transferTable =stateTable.flatMapToPair( pair -> 
                {
                    String hashedUrlself=pair._1();
                    String rankURL = pair._2();
                    String[] parts = rankURL.split(",", 3); // 限制分割的部分数量为3，以确保L不被分割
                    String rc = parts[0]; // 当前排名
                    String rp = parts[1]; // 前一个排名
                    String L = parts[2]; // URL列表的字符串表示
                    // 第二步: 分割L部分以获取单独的URL
                    String[] urls = L.split(",");
                    Set<String> hashedUrls = new HashSet<>();
                    Set<FlamePair> pairs = new HashSet<>();
                    for(String url:urls)
                    {
                        String hashedUrl = Hasher.hash(url); // 假设这个方法返回URL的哈希值
                        hashedUrls.add(hashedUrl);
                    }
                    int n = hashedUrls.size();
                    // 计算每个链接应该接收的排名值
                    double v = 0.85 * (Double.parseDouble(rc) )/ n;
                    Boolean selfLink = false;
                    for(String hashedurl:hashedUrls)
                    {	   	
                        if (hashedurl.equals(hashedUrlself))
                        {selfLink = true;}
                        FlamePair flamePair =new FlamePair(hashedurl,Double.toString(v));
                        pairs.add(flamePair);
                        
                    }
                    // add self-loop with rank 0.0 to prevent vertexes with indegree zero from disappearing
                    if(!selfLink)
                    {
                        FlamePair flamePair =new FlamePair(hashedUrlself,Double.toString(0.0));
                        pairs.add(flamePair);
                    }
                    return pairs;
                });
                //FlamePairRDD transferTable = transferTableOld.foldByKey("0.0",(a,b) -> ""+(Double.parseDouble(a)+Double.parseDouble(b)));
                FlamePairRDD aggregatedTransferTable=transferTable.foldByKey("0.0", (u1, u2) -> 
                {
                    double result = Double.parseDouble(u1) + Double.parseDouble(u2);
                    String resultAsString = Double.toString(result);
                    return resultAsString;
                });

                // join() joins the current PairRDD A with another PairRDD B. Suppose A contains
                // a pair (k,v_A) and B contains a pair (k,v_B). Then the result should contain
                // a pair (k,v_A+","+v_B).
                //public FlamePairRDD join(FlamePairRDD other) throws Exception
                //StateTable(u,"1.0,1.0,L")
                //aggregatedTransferTable (u,v)
                //(u,"1.0,1.0,L",v)
                FlamePairRDD joinedStateTable = stateTable.join(aggregatedTransferTable);
                FlamePairRDD newStateTable=joinedStateTable.flatMapToPair( pair -> 
                {
                    String hashedUrlself=pair._1();
                    String[] fields = pair._2().split(",");
                    Double rc = Double.parseDouble(fields[0]);//current rank
                    Double rp = Double.parseDouble(fields[1]);//previous rank
                    rp=rc;
                    rc= Double.parseDouble(fields[fields.length-1])+0.15;
                    Set<FlamePair> pairs = new HashSet<>();
                    //提取field数组中的URL从index=2开始到index=fields.length-2不包括最后一个元素
                    String outLinks = String.join(",",  Arrays.copyOfRange(fields, 2, fields.length-1));
                    pairs.add(new FlamePair(hashedUrlself, rc + "," + rp + "," + outLinks));
                    return pairs;
                });

                stateTable=newStateTable;
                // flatMap() should invoke the provided lambda once for each pair in the PairRDD, 
                // and it should return a new RDD that contains all the strings from the Iterables 
                // the lambda invocations have returned. It is okay for the same string to appear 
                // more than once in the output; in this case, the RDD should contain multiple 
                // copies of that string. The lambda is allowed to return null or an empty Iterable.
                //public FlameRDD flatMap(PairToStringIterable lambda) throws Exception
                long totalUrls = newStateTable.collect().size();
                FlameRDD rankChange=newStateTable.flatMap(pair -> 
                {
			            String[] fields = pair._2().split(",");
			            double currentRank = Double.parseDouble(fields[0]);
			            double previousRank = Double.parseDouble(fields[1]);
                        // ArrayList<String> changes = new ArrayList<>();
                        // changes.add(String.valueOf(Math.abs(currentRank - previousRank)));
                        //return changes;
                        boolean isConvergent = Math.abs(currentRank - previousRank) <= convergenceThreshold;
                        return new Iterable<String>() {
                            @Override
                            public Iterator<String> iterator()
                            {
                                return Collections.singletonList(isConvergent ? "1" : "0").iterator();
                            }
                        };
                });
                //public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception;
                // String maximumChange=rankChange.fold("0.0", (a, b) -> ""+Math.max(Double.parseDouble(a), Double.parseDouble(b)));
                // System.out.println("maxChange: " + maximumChange);
                //delete some unnessary table
                long convergentCount = rankChange.filter(s -> s.equals("1")).count();

                double currentConvergencePercentage = 100.0 * convergentCount / totalUrls;
                System.out.println("Current convergence percentage: " + currentConvergencePercentage);
        
                rankChange.destroy();
                joinedStateTable.destroy();
                aggregatedTransferTable.destroy();
        
                // // Check for convergence
			    // if (Double.parseDouble(maximumChange) < convergenceThreshold) 
                // {
			    //     break;
			    // }
                if (currentConvergencePercentage >= convergencePercentage) {
                    break;
                }
            }
            //把最后的rank情况存入表中
            KVSClient kvs = new KVSClient(masterAddr);
            List<FlamePair> statelist = stateTable.collect();

            for (FlamePair item : statelist) {
                String url = item._1();
                String[] fields = item._2().split(",");
			    Double currentRank = Double.parseDouble(fields[0]);
                kvs.put("pt-pageranks", url, "rank", String.valueOf(currentRank));
            }

            System.out.println("Number of Iterations: " + numberOfIterations);
			context.output("OK");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
