package PhiMark;

import com.alibaba.fastjson.JSONObject;
import com.opencsv.CSVWriter;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Vector;

/**
 * General utility methods
 */
public class Util {
    /**
     * Hash the attribute values using MD5, returning the result as a BigInteger
     * @param data
     * @throws NoSuchAlgorithmException
     */
    public static BigInteger getMd5Result(String data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte md5[] = md.digest(data.getBytes(StandardCharsets.UTF_8));//Perform MD5 hash computation on the data
        String result = "";
        for(byte b : md5)
        {
            result = result + Integer.toHexString(b & 0xff);//Convert the range -128 to 127 into 0 to 255
        }

        BigInteger res = new BigInteger(result,16);
        return res;
    }

    /**
     * Convert the string into its corresponding binary form using Unicode encoding.
     * @param str
     * @return
     */
    public static String strToUnicode(String str){
        StringBuffer unicode = new StringBuffer();
        for (int i = 0; i < str.length(); i++) {//Convert each character in the string to its binary form and concatenate them.
            char c = str.charAt(i);
            unicode.append((Integer.toBinaryString(c+ 0x10000)).substring(1));
            //Ensure that each character in the String corresponds to 16 bits in the resulting binary string for easy decoding.
        }
        return String.valueOf(unicode);
    }

    /**
     * Convert a string to binary form
     * @param str
     * @returna binary string
     */
    public static String strToAscii(String str){
        StringBuilder binary = new StringBuilder();
        for (char c : str.toCharArray()) {
            String bin = Integer.toBinaryString(c + 0x100).substring(1);//确保不会’0‘不会被舍去，舍得恢复出来一个字符对应八位
            binary.append(bin);
        }
        return binary.toString();
    }

    /**
     * Convert the binary string to a String
     * @param binaryString binary string
     * @return String
     */
    public static String unicodeToString(String binaryString) {
        int length = binaryString.length();
        byte[] bytes = new byte[length / 8];//Convert the binary string into a byte array
        for (int i = 0; i < length; i += 8) {
            String byteString = binaryString.substring(i, i + 8);
            bytes[i / 8] = (byte) Integer.parseInt(byteString, 2);
        }
        return new String(bytes,java.nio.charset.StandardCharsets.UTF_16);
    }

    /**
     * Convert binary ASCII code to a string
     * @param binaryString
     * @return
     */
    public static String asciiToString(String binaryString) {
        StringBuilder ascii = new StringBuilder();
        for (int i = 0; i < binaryString.length(); i += 8) {//One character corresponds to 8bits.
            String byteString = binaryString.substring(i, i + 8); // Get 8 bits
            int asciiCode = Integer.parseInt(byteString, 2);
            ascii.append((char) asciiCode); // Append character to string
        }
        return ascii.toString(); // Return final string
    }

    /**
     * Output the attacked dataset to a CSV file
     * @param fillPath   The path of the CSV file
     * @param d    Attacked dataset
     * @throws IOException
     */
    public static void toCsv(String fillPath,Data d) throws IOException {
        File testFile = new File(fillPath);
        if (!testFile.exists()) {
            testFile.createNewFile();
        }

        BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(fillPath));
        CSVWriter writer = new CSVWriter(bufferedWriter);
        String[] name = new String[d.name.size()];
        for(int i = 0;i<d.name.size();i++){
            name[i] = d.name.get(i);
        }
        writer.writeNext(name);
        String[] tmp = new String[d.d.size()];
        for(int i = 0;i<d.d.get(0).size();i++){
            for(int j = 0;j<d.d.size();j++){
                tmp[j]=d.d.get(j).get(i);
            }
            writer.writeNext(tmp);
        }
        writer.close();
        bufferedWriter.close();
    }


    /**
     * Store the resulting validation information in json format to a file
     * @param filePath
     * @param jsonObject
     */
    public static void writeToJson(String filePath, JSONObject jsonObject){
        try (FileWriter file = new FileWriter(filePath)) {
            file.write(jsonObject.toJSONString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Read information from a json file
     * @param filePath
     * @return  the read information as a string str
     * @throws IOException
     */
    public static String readFromJson(String filePath) throws IOException {
        try {
            FileInputStream fin = new FileInputStream(filePath);
            InputStreamReader reader = new InputStreamReader(fin);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String str = bufferedReader.readLine();  //Record the read information as a string str
            bufferedReader.close();
            reader.close();
            fin.close();
            return str;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     *
     * @param jsonObject
     * @param name
     * @param clazz
     * @return
     * @param <T>
     */
    public static<T> Vector<T> fromjson(JSONObject jsonObject,String name,Class<T> clazz){
        Vector<T> vec = new Vector<>();
        String tmp = (String) jsonObject.get(name);
        String[] strs = tmp.split("我");
        for(String str : strs){
            if (clazz == Integer.class) {
                vec.add(clazz.cast(Integer.parseInt(str)));
            }
            else if(clazz == Double.class){
                vec.add(clazz.cast(Double.parseDouble(str)));
            }
            else if(clazz == StringBuffer.class){
                vec.add(clazz.cast(new StringBuffer(Util.strToAscii(str))));
            }
        }
        return vec;
    }

    /**
     * Store a vector as a string in JSON, with different values separated by spaces " "
     * @param jsonObject
     * @param vec    The vector to be stored in JSON
     * @param name   The corresponding name in JSON
     */
    public static void tojson(JSONObject jsonObject, Vector<StringBuffer> vec, String name){
        String tmp = "";
        for(int i = 0;i<vec.size();i++){
            tmp+=Util.asciiToString(String.valueOf(vec.get(i)));//Convert a binary string to a string, saving storage space
            tmp+="我";//different values separated by spaces " "
        }
        jsonObject.put(name,tmp);
    }


    /**
     * Obtain the XOR result
     * @param m
     * @param str
     * @return
     */
    public static StringBuffer xxor(StringBuffer m,String str){
        StringBuffer res = new StringBuffer(str.length());
        for(int i = 0;i<str.length();i++){
            if(m.charAt(i) == str.charAt(i)){
                res.append('0');
            }
            else{
                res.append('1');
            }
        }
        return res;
    }


    /**
     *
     * @return
     */
    public static char getRandomAlpha(){
        return (char) (Math.random()*26+97);
    }
    /**
     *  Generate a randomly generated lowercase letter string
     * @param size  length of string
     */
    public static String getRandomAlphaString(int size){
        StringBuffer stringBuffer = new StringBuffer();

        for (int i = 0; i < size; i++) {
            stringBuffer.append(getRandomAlpha());
        }
        return stringBuffer.toString();
    }
}
