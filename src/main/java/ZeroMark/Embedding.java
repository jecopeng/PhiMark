package ZeroMark;

import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class Embedding {
    public static void main(String[] args) throws CsvValidationException, SQLException, IOException, NoSuchAlgorithmException {
        watermark wm = new watermark();
        wm.encode("queaterlywages2000");//for QCEW dataset.
        //wm.encode("geography");
    }
}
