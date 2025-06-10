package Merge;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Vector;

public class merge {
    @Test
    public void test() throws CsvValidationException, IOException {
        //String dbName = "geography";
        //String dbName = "queaterlywages2000";
        String dbName = "Reviews";
        merge(dbName);

    }
    public void merge(String dbName) throws IOException, CsvValidationException {
        String inputFolder = "database\\" + dbName; // 替换为你的CSV文件夹路径
        String outputFile = "database\\" + dbName + ".csv";

        File folder = new File(inputFolder);
        File[] files = folder.listFiles();

        Vector<String[]> vectors = new Vector<>();

        if (files != null) {
            for (File file : files) {
                CSVReader reader = new CSVReader(new FileReader(file));
                String[] line;
                while ((line = reader.readNext()) != null) {
                    vectors.add(line);
                }
            }
        }

        BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(outputFile));
        CSVWriter writer = new CSVWriter(bufferedWriter);
        for(int i = 0;i<vectors.size();i++){
            writer.writeNext(vectors.get(i));
        }
        writer.close();
        bufferedWriter.close();

    }
}
