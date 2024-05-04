package cis5550.jobs;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


public class DictionaryFilter {
    public static Set<String> loader(){
        String filePath = "words_alpha.txt";
        Set<String> wordSet = new HashSet<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            wordSet = reader.lines().parallel()
                .flatMap(line -> Arrays.stream(line.split("\\s+")))
                .collect(Collectors.toSet());
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
        return wordSet;
    }
    
}
