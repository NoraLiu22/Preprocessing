package cis5550.jobs;


import java.io.IOException;
import java.util.Arrays;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


public  class StopWordsLoader {
    //public static Set<String> stopwords;
    public static Set<String>  stopWords()
    {   Set<String> stopwords = new HashSet<>();
        try {
            //Use HashSet to store StopWords
             stopwords = Files.readAllLines(Paths.get("src/cis5550/jobs/stopWords.txt"))
                    .stream()
                    .flatMap(line -> Arrays.stream(line.split(","))) // 将split方法的结果转换为Stream
                    .map(String::trim) // 去除首尾空格 .replace("'", "")
                    .map(word -> word.replace("\"", "")) // 去除所有单引号和双引号
                    .collect(Collectors.toCollection(HashSet::new)); // 明确使用HashSet
            // String wordToFind = "0"; // 可以替换成您要查找的词
            // if (stopwords.contains(wordToFind)) {
            //     System.out.println(wordToFind + "is a stopword");
            //     } else
            //     {
            //          System.out.println(wordToFind + " is not a stopword");
            //     }
            // System.out.println("StopWordsList:");
            // stopwords.forEach(System.out::println);
            // int stopwordsCount = stopwords.size();
            // System.out.println("Num of StopWords: " + stopwordsCount);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return stopwords;
    }
    
}