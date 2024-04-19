package cis5550.jobs;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
public class HtmlCleaner {
    // List arrays for different headers
    // List<String> h1Words = new ArrayList<>();
    // List<String> h2Words = new ArrayList<>();
    // List<String> h3Words = new ArrayList<>();
    // List<String> h4Words = new ArrayList<>();
    // List<String> h5Words = new ArrayList<>();
    // List<String> h6Words = new ArrayList<>();
//HashSet<String> h3Words,HashSet<String> h4Words,HashSet<String> h5Words,HashSet<String>h6Words
    public String clean(String html,HashSet<String> Title,HashSet<String> h1Words, HashSet<String> h2Words) 
    {
        
        Document doc = Jsoup.parse(html);
        // 移除特定的标签及其内容
        doc.select("meta, script, link").remove();
        // 先提取标题，因为稍后我们会从文档中移除title标签
        String title = doc.title();
        Title.add(title);
        // 移除title标签
       // doc.select("title").remove();
        // Process headers h1 through h6
        for (int i = 1; i <= 6; i++) 
        {
            Elements headers = doc.select("h" + i);
            for (Element header : headers) 
            {
                String[] words = header.text().split("\\s+");
                for (String word : words) 
                {
                    switch (i) {
                        case 1:
                            h1Words.add(word);
                            break;
                        case 2:
                            h2Words.add(word);
                            break;
                        // case 3:
                        //     h3Words.add(word);
                        //     break;
                        // case 4:
                        //     h4Words.add(word);
                        //     break;
                        // case 5:
                        //     h5Words.add(word);
                        //     break;
                        // case 6:
                        //     h6Words.add(word);
                        //     break;
                    }
                }
                //header.remove();
            }
        }
       
        // 获取整个文档的纯文本，此时已经移除了不需要的标签
        String text = doc.text();

        // 进一步处理文本：移除标点、非字母数字字符、非ASCII字符、空白字符
        text = text.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
        text = text.replaceAll("[^a-zA-Z]", " ");
        text = text.replaceAll("[^\\p{ASCII}]", " ");
        text = text.replaceAll("[\\r\\n\\t]+", " ");
        text = text.toLowerCase();
        return text;
        //System.out.println(text);
    }
    public String clean2(String html)
    {
        Document doc = Jsoup.parse(html);
        // 移除特定的标签及其内容
        doc.select("meta, script, link").remove();
        // 获取整个文档的纯文本，此时已经移除了不需要的标签
        String text = doc.text();
        // 进一步处理文本：移除标点、非字母数字字符、非ASCII字符、空白字符
        text = text.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
        text = text.replaceAll("[^a-zA-Z]", " ");
        text = text.replaceAll("[^\\p{ASCII}]", " ");
        text = text.replaceAll("[\\r\\n\\t]+", " ");
        text = text.toLowerCase();
        return text;
    }
}
