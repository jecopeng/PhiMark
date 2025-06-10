package PhiMark;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomTokenizer {
    public static void main(String[] args) {
        String text = "This is an example: \"OpenNLP...is great,\" isn't it? And here's of_all_time.";

        // 定义正则表达式，匹配下划线连接的词组、单词和逗号、冒号、...
        String regex = "\\w+'\\w+|\\w+|[,:\"]|\\.\\.\\.";

        // 创建 Pattern 和 Matcher 对象
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);

        // 使用 List 保存分词结果
        List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }

        // 输出分词结果
        for (String token : tokens) {
            System.out.println(token);
        }

        String[] token = tokens.toArray(new String[0]);
        System.out.println(token.length);
    }

    public static String[] tokenizer(String text){
        // 定义正则表达式，匹配下划线连接的词组、单词和逗号、冒号
        String regex = "\\w+'\\w+|\\w+|[,:\"]|\\.\\.\\.";

        // 创建 Pattern 和 Matcher 对象
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);

        // 使用 List 保存分词结果
        List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }

        return tokens.toArray(new String[0]);
    }
}
