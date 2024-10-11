package ZeroMark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Random;
import java.util.Vector;

/**
 *  the core algorithms for watermark embedding and extraction
 */
public class watermark {
    /**
     * Embedding watermarks for a dataset
     * @param dataName    Name of the dataset
     * @throws CsvValidationException
     * @throws SQLException
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public void encode(String dataName) throws CsvValidationException, SQLException, IOException, NoSuchAlgorithmException {
        String csvPath = "database\\" + dataName + ".csv";
        Data d = new Data(csvPath);//read in data from a CSV file (for QCEW dataset).
        //String watermark = generateWatermark(1000);//Randomly generate a 1000-bit binary string as a watermark.

        //In practical applications, meaningful copyright information is often used as a watermark.
        //In this case, the ownership of the data can be directly determined based on the extracted watermark information.
        String watermark = generateWatermark("ACM SIGMOD/PODS International Conference on Management of Data, June 22-27, 2025 Berlin, Germany");
        int len = watermark.length();

        //long stime = System.currentTimeMillis();//Test the time overhead of embedding watermarks;
        Vector<Integer> parameters = genetics.getGene(d,len);

        String key1 = "ACM";//SK1
        String key2 = "SIGMOD";//SK2

        Vector<Vector<Vector<Integer>>> partitions = getAllPartitions(parameters,d,key1);//Get the index of the selected value in each partition.
        int t = computeMasks(parameters); //Calculate the total number of verification bit strings to store.
        Vector<StringBuffer> Code = getAllVeri(t,partitions,key2,d,watermark);//Generate verfication information for all partitions.
        //long etime = System.currentTimeMillis();
        //System.out.println(etime - stime);


        JSONObject jsonObject = new JSONObject();//The information is stored locally in JSON format.

        //Store the relevant information of the watermark attributes
        String M = "";
        String E = "";
        String name = "";
        /*Store each attribute name along with its corresponding partition count m and embedding interval e respectively,
        ensuring a one-to-one correspondence.*/
        /*In this way, even if an attribute is deleted, we can still identify the deleted attribute and its parameters,
        ensuring that watermark extraction for the remaining attributes is not affected.*/
        //Therefore, even if only one column of watermark attributes remains, ZeroMark can still successfully extract the watermark.
        //Parameters of different attributes are separated by spaces.
        for(int i = 0;i<partitions.size();i++){
            name+=d.name.get(i);
            name+=" ";
            M+=parameters.get(2*i);
            M+=" ";
            E+=parameters.get(2*i+1);
            E+=" ";
        }
        jsonObject.put("name",name);//Store the parameter string into a JSON object
        jsonObject.put("M",M);
        jsonObject.put("E",E);

        jsonObject.put("Key1",key1);
        jsonObject.put("Key2",key2);
        //If the watermark is derived from meaningful information, it may not be necessary to store the watermark.
        jsonObject.put("Watermark",watermark);

        Util.tojson(jsonObject,Code,"Code");//Store the verification information into JSON
        //For simplicity, we store the information in a txt file here.
        //In practical applications, all information should be stored in a database table.
        //The stored information of a dataset corresponds to a tuple in the table.
        String jsonPath = "veri Information\\" + dataName+"veriInfo.json";
        Util.writeToJson(jsonPath,jsonObject);
    }

    /**
     * Extracting watermarks from a dataset
     * @param dataName     Name of the dataset
     * @throws CsvValidationException
     * @throws SQLException
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public void decode(String dataName) throws CsvValidationException, SQLException, IOException, NoSuchAlgorithmException {
        Vector<int[]> com = new Vector<>();//Record which attributes form the composite attribute
        Vector<StringBuffer> Code = new Vector<>();
        Vector<Integer> parameters = new Vector<>();
        String[] Keys = new String[2];

        String jsonInput = null;
        String jsonPath = "veri Information\\" + dataName +"veriInfo.json";
        jsonInput = Util.readFromJson(jsonPath);
        JSONObject jsonObject2 = JSON.parseObject(jsonInput);
        Keys[0] = jsonObject2.getString("Key1");//SK1
        Keys[1] = jsonObject2.getString("Key2");//SK2
        Code = Util.fromjson(jsonObject2,"Code",StringBuffer.class);//Verification information for all partitions.
        String watermark = jsonObject2.getString("Watermark");

        String[] names = jsonObject2.getString("name").split(" ");

        String M = jsonObject2.getString("M");//Partition count set
        String E = jsonObject2.getString("E");//Embedding interval set

        String[] m = M.split(" ");
        String[] e = E.split(" ");
        parameters = new Vector<>();
        for(int i = 0;i<m.length;i++){
            //Obtain the set of all attribute partition numbers m and embedding intervals e.
            //The parameters at index 2*i corresponds to the m of the i-th watermark attribute,
            //and 2i+1 corresponds to the e of the i-th watermark attribute
            parameters.add(Integer.parseInt(m[i]));
            parameters.add(Integer.parseInt(e[i]));
        }


        Data d = new Data("attacked database\\data.csv",names,parameters,Code);//data.csv is the copyright-dispute dataset
        //Data d = new Data("database\\" + dataName+".csv");
        String res = decodeWatermark(d,Keys[0],Keys[1],parameters,Code,watermark.length());

        if(res.equals(watermark)){
            System.out.println("matching");

            System.out.println(Util.unicodeToString(res));
        }
        int count = 0;
        for(int i = 0;i<res.length();i++){
            if(res.charAt(i)==watermark.charAt(i)){
                count++;
            }
        }
        System.out.println((double)count/res.length());
    }



     /**
     * Calculate the total number of verification bit strings to store.
     * @param paras The set of all attribute partition numbers m and embedding intervals e.
      *             The parameters at index 2*i corresponds to the m of the i-th watermark attribute,
      *             and 2i+1 corresponds to the e of the i-th watermark attribute
     * @return
      */
    public int computeMasks(Vector<Integer> paras){
        int res= 0;
        for(int i = 0;i<paras.size();i+=2){
            res+=paras.get(i);
        }
        return res;
    }

     /**
     * Data partitioning for a specific watermark attribute.
     * @param m     Partition count
     * @param str1  The attribute is partitioned
     * @param key   Secret key : key1
     * @param e     Embedding interval
     * @return partitions
     * @throws NoSuchAlgorithmException
      */
    public Vector<Vector<Integer>> getPartitions(int m, Vector<String> str1, String key, int e) throws NoSuchAlgorithmException {//进行初始分区，m表示最终的分组数
        Vector<Vector<Integer>> vec = new Vector<Vector<Integer>>(m);
        for(int i = 0;i<m;i++){//Initialize m partitions
            Vector<Integer> partition = new Vector<Integer>();
            vec.add(partition);
        }

        for(int i = 0;i<str1.size();i++){//Data partitioning
            String str = str1.get(i);
            str += key;
            BigInteger bigInteger1 = Util.getMd5Result(str);// Hash(str[i]||SK1)
            if(bigInteger1.mod(new BigInteger(Integer.toString(e))).intValue()==0){//str[i] is selected as a watermark value
                str = bigInteger1.toString();
                str = key + str;
                BigInteger bigInteger = Util.getMd5Result(str);
                int partition = bigInteger.mod(BigInteger.valueOf(m)).intValue();
                vec.get(partition).add(i);//Place str[i] into the corresponding partition
            }
        }
        return vec;
    }

     /**
     * Perform data partitioning for each attribute separately
     * @param paras The set of all attribute partition numbers m and embedding intervals e.
      *             The parameters at index 2*i corresponds to the m of the i-th watermark attribute,
      *             and 2i+1 corresponds to the e of the i-th watermark attribute
     * @param d
     * @param key   SK1
     * @return   The index of the selected value in each partition.
     * @throws NoSuchAlgorithmException
      */
    public Vector<Vector<Vector<Integer>>> getAllPartitions(Vector<Integer> paras, Data d, String key) throws NoSuchAlgorithmException {
        Vector<Vector<Vector<Integer>>> vec = new Vector<>();
        for(int i = 0;i<d.d.size();i++){
            //the parameters at index 2*i corresponds to the m of the i-th watermark attribute,
            // and 2i+1 corresponds to the e of the i-th watermark attribute
            vec.add(getPartitions(paras.get(2*i),d.d.get(i),key,paras.get(2*i+1)));
        }
        return vec;
    }

     /**
     * Randomly generate a watermark in the form of a fixed-length binary string
     * @param size   The length of watermark
     * @return
      */
    public String generateWatermark(int size){
        StringBuffer str = new StringBuffer("");
        Random random = new Random();
        for(int i = 0;i<size;i++){
            str.append(random.nextInt(2));
        }

        String res = str.toString();
        return res;
    }

    /**
     * Convert the watermark information into a binary string
     * @param watermark
     * @return The binary representation of the watermark string
     */
    public String generateWatermark(String watermark){
        return Util.strToUnicode(watermark);
    }


     /**
     * Extract features from each group to generate masks, and use the masks for watermark embedding and extraction
     * @param len   length of watermark
     * @param vec   The indices of selected values for watermarking in this partition
     * @param key2
     * @param dstr  a watermark attribute
     * @param watermark   In the embedding process, it refers to the watermark,
     *                    while in the extraction process, it refers to the verification information corresponding to the partition.
     * @return
     * @throws NoSuchAlgorithmException
      */
    public StringBuffer getMaskCode(int len,Vector<Integer> vec,String key2,Vector<String> dstr,String watermark) throws NoSuchAlgorithmException {
        StringBuffer markCode = new StringBuffer(len);
        for(int i = 0;i<len;i++){//Initialize a mask string of length len filled with zeros
            markCode.append(0);
        }
        for(int i = 0;i<vec.size();i++){
            String str = dstr.get(vec.get(i)) + key2;
            BigInteger bigInteger1 = Util.getMd5Result(str);//Hash(dstr[i] || SK2)
            str = bigInteger1.toString();
            str = key2 + str;
            bigInteger1 = Util.getMd5Result(str);
            int res = Integer.parseInt(bigInteger1.mod(new BigInteger(len+"")).toString());
            markCode.setCharAt(res,'1');//Set the corresponding bit at the marked positions to 1
        }

        return Util.xxor(markCode,watermark);//
    }

     /**
     * Calculate the verification information for all partitions
     * @param num    The sum of the number of partitions for all watermark attributes
     * @param groups The indices of the selected values in each partition.
     * @param key2   SK
     * @param d
     * @param watermark
     * @return    The verification information for all partitions
     * @throws NoSuchAlgorithmException
      */
    public Vector<StringBuffer> getAllVeri(int num,Vector<Vector<Vector<Integer>>> groups,String key2,Data d,String watermark) throws NoSuchAlgorithmException{
        Vector<StringBuffer> res = new Vector<>(num);//Initialize the verification information set
        for(int i = 0;i<d.d.size();i++){
            for(int j = 0;j<groups.get(i).size();j++){
                res.add(getMaskCode(watermark.length(),groups.get(i).get(j),key2,d.d.get(i),watermark));
                //Calculate the verification bit string for the j-th partition of the i-th attribute
            }
        }
        return res;
    }


     /**
     * extract the watermark
     * @param d        Attacked dataset
     * @param key1     SK1
     * @param key2     SK2
     * @param paras    Parameters stored locally (partition count m and embedding interval e)
     * @param veriinfor    The verification information for all partitions
     * @param len      The length of watermark
     * @return         The extracted result.
     * @throws NoSuchAlgorithmException
      */
    public String decodeWatermark(Data d, String key1, String key2,Vector<Integer> paras,Vector<StringBuffer> veriinfor,int len) throws NoSuchAlgorithmException{
        int t = 0;//存储目前为止组数
        Vector<Vector<Vector<Integer>>> groups = getAllPartitions(paras,d,key1);
        Vector<StringBuffer> res = new Vector<>();
        for(int i = 0;i<groups.size();i++){
            for(int j = 0;j<groups.get(i).size();j++){
                res.add(getMaskCode(len,groups.get(i).get(j),key2,d.d.get(i),String.valueOf(veriinfor.get(t+j))));
            }
            t+=groups.get(i).size();
        }


        String str = "";
        int[] count0 = new int[len];//Below steps are for the majority vote.
        int[] count1 = new int[len];
        for(int i = 0;i<t;i++){
            for(int j = 0;j<len;j++){
                if(res.get(i).charAt(j) == '0'){
                    count0[j]++;
                }
                else{
                    count1[j]++;
                }
            }
        }

        for(int i = 0;i<len;i++){
            if(count1[i]>=count0[i]){
                str = str + "1";
            }
            else{
                str = str + "0";
            }
        }
        return str;
    }
}
