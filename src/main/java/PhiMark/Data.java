package PhiMark;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

/**
 * Data stores the data and related information
 */
public class Data {
    Vector<Vector<String>> d;//Record the data for each attribute

    Vector<String> name;//Record the name for each attribute.
    Vector<int[]> combine;//The watermarking attributes include which columns (possibly more than one, as may be a combination of attribute）
    int[] len;//the number of unique values for each attribute
    public Vector<HashMap<String,Vector<Integer>>> co;//Record the positions (indices) where each unique value appears for each attribute.
    //The key representsa unique value in an attribute, (Vector<Integer>) stores the positions (indices) where this unique value appears.
    public Vector<List<Map.Entry<String,Vector<Integer>>>> entryList;//Record the unique values for each attribute, sorted by frequency.

    /**
     * Sort the frequency of each unique value in each attribute to facilitate high-frequency modification attacks.
     */
    public void sort_count(){
        entryList = new Vector<>(d.size());
        for(int i = 0 ;i<co.size();i++){
            List<Map.Entry<String,Vector<Integer>>> entrys = new ArrayList<>(co.get(i).entrySet());
            Collections.sort(entrys,new MyComparator());
            entryList.add(entrys);
        }
    }

    /**
     *  For each attribute, record all the positions (indices) where each unique value appears.
     */
    public void compute_count(){
        co = new Vector<>(d.size());
        for(int i = 0;i<d.size();i++){
            HashMap<String,Vector<Integer>> hashMap = new HashMap<>();
            for(int j = 0;j<d.get(i).size();j++){
                if(hashMap.get(d.get(i).get(j))==null){
                    Vector<Integer> vec = new Vector<>();
                    vec.add(j);
                    hashMap.put(d.get(i).get(j),vec);
                    continue;
                }
                hashMap.get(d.get(i).get(j)).add(j);
            }
            co.add(hashMap);
        }
    }



    /**
     * Data is read from a CSV file to create the dataset. To simplify the experiment, a CSV file is used as the data source;
     * however, any readable structured data storage format, such as .sql files or other similar formats, can also be used.
     * @param str The path of the CSV file containing the dataset
     * @throws SQLException
     */
    public Data(String str) throws SQLException, IOException, CsvValidationException {
        combine = new Vector<>();//Test, assuming these attributes require watermarking.
        combine.add(new int[]{0});//for queaterlywages2000.csv
        combine.add(new int[]{1});
        combine.add(new int[]{2});

        /*combine.add(new int[]{0});//for geography.csv
        combine.add(new int[]{1});
        combine.add(new int[]{2});*/

        d = new Vector<>(combine.size());//
        for(int i = 0;i< combine.size();i++){
            d.add(new Vector<>());
        }

        name = new Vector<>(combine.size());

        Reader reader = Files.newBufferedReader(Paths.get(str));
        CSVReader csvReader = new CSVReader(reader);
        String[] record;

        record = csvReader.readNext();//The first row containing the attribute names.
        for(int i = 0;i< combine.size();i++){//
            String tmp = "";
            for(int j = 0;j<combine.get(i).length;j++){
                tmp += record[combine.get(i)[j]];
                if(j!=combine.get(i).length-1){//Combine attribute names with different attribute names separated by “,”
                    tmp+=",";
                }
            }
            name.add(tmp);
        }
        while ((record = csvReader.readNext()) != null) {//Read data from a CSV file.
            Vector<String> tmp = new Vector<>(record.length);
            for(int t = 0; t<combine.size();t++){
                String addString = "";
                for(int i = 0;i<combine.get(t).length;i++){//Generate combined attributes（if exists）
                    addString+=record[combine.get(t)[i]];
                }
                d.get(t).add(addString);
            }
        }
        getLen();
    }

    /**
     * Read a portion of the tuples to facilitate testing on datasets of different scales.
     * @param str   The path of the CSV file containing the dataset
     * @param num   The number of tuples to be read in
     * @throws SQLException
     * @throws IOException
     * @throws CsvValidationException
     */
    public Data(String str, int num) throws SQLException, IOException, CsvValidationException{
        int count = 0;//Record the number of tuples read in
        combine = new Vector<>();//Test, assuming these attributes require watermarking.
        combine.add(new int[]{0});//for queaterlywages2000.csv
        combine.add(new int[]{1});
        combine.add(new int[]{2});

        /*combine.add(new int[]{0});//for geography.csv
        combine.add(new int[]{1});
        combine.add(new int[]{2});*/

        d = new Vector<>(combine.size());
        for(int i = 0;i< combine.size();i++){
            d.add(new Vector<>());
        }

        name = new Vector<>(combine.size());

        Reader reader = Files.newBufferedReader(Paths.get(str));
        CSVReader csvReader = new CSVReader(reader);
        String[] record;

        record = csvReader.readNext();//Skip the first row containing the attribute names.
        for(int i = 0;i< record.length;i++){
            name.add(record[i]);
        }
        while ((record = csvReader.readNext()) != null) {//Read data from a CSV file.
            count++;
            Vector<String> tmp = new Vector<>(record.length);
            for(int t = 0; t<combine.size();t++){
                String addString = "";
                for(int i = 0;i<combine.get(t).length;i++){//Generate combined attributes（if exists）
                    addString+=record[combine.get(t)[i]];
                }
                d.get(t).add(addString);
            }
            if(count == num){
                break;
            }
        }
        getLen();

    }

    /**
     * Extract watermark attributes from the attacked dataset based on the attribute names.
     * @param str
     * @param names
     * @throws SQLException
     * @throws IOException
     * @throws CsvValidationException
     */
    public Data(String str,String[] names, Vector<Integer> paras,Vector<StringBuffer> code) throws SQLException, IOException, CsvValidationException {
        Reader reader = Files.newBufferedReader(Paths.get(str));
        CSVReader csvReader = new CSVReader(reader);
        String[] record;
        record = csvReader.readNext();


        combine = new Vector<>(names.length);
        int count = 0;//Record the number of watermark attributes that cannot be found
        int co = 0;//Record the total number of partitions for the processed attributes
        //Find the index of the attributes corresponding to the combination attribute using the attribute name
        for(int i = 0;i<names.length;i++){

            String[] tmp = names[i].split(",");
            int[] t = new int[tmp.length];
            int num = 0;
            boolean bool = true;
            //Match the stored attribute names with the read attribute names to obtain the index of the watermark attributes
            for(String s:tmp){
                int j = 0;
                for(j = 0;j< record.length;j++){
                    if(record[j].equals(s)){
                        t[num++] = j;
                        break;
                    }
                }
                //This attribute has been removed, and during reading, both the attribute and its verification information are skipped.
                if(j == record.length){
                    for(int p = co;p<co+paras.get(2*(i-count));p++){
                        code.remove(co);
                    }
                    paras.remove(2*(i-count));
                    paras.remove(2*(i-count));
                    bool = false;
                    count++;
                    break;
                }
            }
            if(bool){
                combine.add(t);
                co+=paras.get(2*(i-count));//Add the number of partitions corresponding to this attribute to the total count
            }
        }

        d = new Vector<>(combine.size());
        for(int i = 0;i< combine.size();i++){//Initialize d
            d.add(new Vector<>());
        }

        while ((record = csvReader.readNext()) != null) {
            for(int t = 0; t<combine.size();t++){
                String addString = "";
                for(int i = 0;i<combine.get(t).length;i++){
                    addString+=record[combine.get(t)[i]];
                }
                d.get(t).add(addString);
            }
        }
        getLen();
    }

    /**
     * Initialize the dataset for the deletion attack and the insertion attack
     * @param size     The number of watermark attributes
     * @param length   The number of tuples
     */
    public Data(int size,int length){
        d = new Vector<>(size);
        for(int i = 0;i<size;i++){
            d.add(new Vector<>(length));
        }
    }


    /**
     * Read the complete dataset information
     * @param str
     * @throws SQLException
     * @throws IOException
     * @throws CsvValidationException
     */
    public Data(String str,char ch) throws SQLException, IOException, CsvValidationException {
        Reader reader = Files.newBufferedReader(Paths.get(str));
        CSVReader csvReader = new CSVReader(reader);
        String[] record;
        record = csvReader.readNext();
        name = new Vector<>(record.length);
        for(int t = 0; t<record.length;t++){
            name.add(record[t]);
        }

        d = new Vector<>(record.length);
        for(int i = 0;i< record.length;i++){//Initialize d
            d.add(new Vector<>());
        }

        while ((record = csvReader.readNext()) != null) {
            for(int t = 0; t<record.length;t++){
                d.get(t).add(record[t]);
            }
        }
    }


    /**
     * Obtain the number of unique values for each attribute
     * @throws SQLException
     */
    public void getLen() throws SQLException{
        len = new int[d.size()];
        for(int i = 0;i<len.length;i++){
            HashSet<String> hashset = new HashSet<>();
            for(String str:d.get(i)){
                hashset.add(str);
            }
            len[i] = hashset.size();

        }

    }


    /**
     * Sort the keys (unique values) based on their frequency (i.e., the number of positions where they appear).
     */
    class MyComparator implements Comparator<Map.Entry<String,Vector<Integer>>>{

        @Override
        public int compare(Map.Entry<String, Vector<Integer>> o1, Map.Entry<String, Vector<Integer>> o2) {
            Integer i1 = o1.getValue().size();
            Integer i2 = o2.getValue().size();
            return i2.compareTo(i1);
        }
    }
}
