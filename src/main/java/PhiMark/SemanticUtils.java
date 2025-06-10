package PhiMark;

import net.didion.jwnl.JWNL;
import net.didion.jwnl.JWNLException;
import net.didion.jwnl.data.IndexWord;
import net.didion.jwnl.data.POS;
import net.didion.jwnl.data.Synset;
import net.didion.jwnl.data.Word;
import net.didion.jwnl.dictionary.Dictionary;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class SemanticUtils {
    /*public static void main(String[] args) throws IOException, JWNLException {
        SemanticUtils utils = new SemanticUtils();
        String testsentence = "I have bought several of the Vitality canned dog food products, and have found them all to be of good quality. The product looks more like a stew than a processed meat and it smells better. My Labrador is finicky and she appreciates this product better than  most.";

        *//*Vector<Wordandtags> vecTmp = utils.segmentationAndFiltering(testsentence);
        String[] tokens = utils.segmentation(testsentence);
        for(Wordandtags wt:vecTmp){
            Synset synset = utils.getBestSense(wt.word,wt.tag,tokens);
            System.out.println(synset.getGloss());
        }*//*
        Vector<Vector<Candidates>> cans = utils.getCandidates(testsentence);
        for(Vector<Candidates> veccan:cans){
            for(Candidates can:veccan){
                for(Word word:can.words){
                    System.out.print(word.getLemma()+" ");
                }
                System.out.println();
            }
        }
    }*/
    Dictionary dictionary;//初始化wordnet词典
    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
            "the", "a", "an", "is", "was", "are", "were", "has", "have", "had", "do", "does", "did",
            "of", "to", "and", "but", "or", "because", "so", "if", "than", "then"
    ));
    public SemanticUtils() throws FileNotFoundException, JWNLException {
        InputStream stream = new FileInputStream("src/main/resources/config.xml");
        JWNL.initialize(stream);
        dictionary = Dictionary.getInstance();
    }



    public void synonymSubstitution(Data d,int i) throws IOException, JWNLException {//i标识执行替换的元组
        Random random = new Random();
            Vector<String> res = new Vector<>();//每一个句子的修改结果
            String[] sens = d.d.get(9).get(i).split("(?<!\\.)[.!?](?!\\.)");//多词属性拆分成单个句子
            for(String sentence:sens){
                Vector<Candidates> vecCan = new Vector<>();

                Vector<Wordandtags> wordandtagsVector = segmentationAndFiltering(sentence);
                String[] context = segmentation(sentence);
                String[] tmps = segmentationDontDelete(sentence);//这个用于恢复原句

                for(Wordandtags wt: wordandtagsVector){
                    Synset bestsense = getBestSense(wt.word,wt.tag,context);
                    if(bestsense !=null){//可以进行同义词替换
                        int pos = random.nextInt(bestsense.getWordsSize());
                        tmps[wt.pos] = bestsense.getWords()[pos].getLemma();
                    }
                }

                StringBuilder replaceSentence = new StringBuilder();
                int num = 0;
                boolean b = false;
                for(String tmp:tmps){
                    if(tmp.matches("[a-zA-Z0-9_()']+")){
                        if(num == 0 || b){//第一个不空格，后面的都先空一格
                            b = false;
                        }
                        else{
                            replaceSentence.append(" ");
                        }

                    }
                    if(tmp.equals("...")){
                        b = true;
                    }

                    else{
                        replaceSentence.append(tmp);
                    }

                    num++;
                }

                res.add(replaceSentence.toString());


            }
            String str = String.join(". ",res);
            str += ".";//给最后加句号，标点没影响，这里就不管了，都加句号了
            d.d.get(9).set(i,str);
    }

    public Synset getBestSense(String word,String speech,String[] context) throws JWNLException {
        IndexWord indexWord = null;
        if(speech.matches("NOUN")){
            indexWord = dictionary.lookupIndexWord(POS.NOUN, word);
        }
        else if(speech.matches("VERB")){
            indexWord = dictionary.lookupIndexWord(POS.VERB, word);
        }
        else if(speech .matches("ADJ")){
            indexWord = dictionary.lookupIndexWord(POS.ADJECTIVE, word);
        }
        else if(speech .matches("ADV")){
            indexWord = dictionary.lookupIndexWord(POS.ADVERB, word);
        }
        //assert indexWord != null;
        if(indexWord == null){//语料里有词性标注错误问题，跳过
            return null;
        }
        Synset[] senses = indexWord.getSenses();

        Synset bestSense = null;
        double maxScore = -1.0;
        for(Synset synset : senses){
            double score = cauculateOverlap(synset.getGloss().split(" "), context,word);
            Word[] words = synset.getWords();
            if (score > maxScore) {
                maxScore = score;
                bestSense = synset;

            }


        }
        return bestSense;
    }

    public int cauculateOverlap(String[] contextWords, String[] senseWords,String word){//计算重叠此，lesk算法就是通过计算重叠词来实现的（意思和上下文之间）
        int overlap = 0;
        for(String contextWord:contextWords){
            if(contextWord.equals(word)){//跳过这个单词，不计算原来的单词的出现次数
                continue;
            }
            for(String senseWord:senseWords){
                if(contextWord.equals(senseWord)){
                    overlap++;
                }
            }
        }
        return overlap;
    }

    public String[] segmentationDontDelete(String sentence) throws IOException {
        return CustomTokenizer.tokenizer(sentence);
    }


    public Vector<Wordandtags> segmentationAndFiltering(String sentence) throws IOException {
        String token[] = CustomTokenizer.tokenizer(sentence);
        FileInputStream modelIn = new FileInputStream("src/main/resources/opennlp-en-ud-ewt-pos-1.2-2.5.0.bin");
        POSModel posModel = new POSModel(modelIn);
        POSTaggerME posTagger = new POSTaggerME(posModel);//词性标注工具

        // 获取词性标注结果
        String[] tags = posTagger.tag(token);
        Vector<Wordandtags> wordandtagsVector = new Vector<>();

        for(int i = 0;i<token.length;i++){//最终的结果中，去除标点符号，以及对句意影响比较小的词性
            if(token[i].matches("[a-zA-Z0-9_]+") && tags[i].matches("NOUN|VERB|ADJ|ADV")){
                wordandtagsVector.add(new Wordandtags(token[i],tags[i],i));
            }
        }
        return wordandtagsVector;
    }

    public String[] segmentation(String sentence) throws IOException {
        String[] context = CustomTokenizer.tokenizer(sentence);
        Vector<String> vec = new Vector<>();
        for(String w : context){//去掉标点符号和停用词
            if (!STOPWORDS.contains(w)&&w.matches("[a-zA-Z0-9_]+")){
                vec.add(w);
            }
        }
        return vec.toArray(new String[vec.size()]);
    }



}
