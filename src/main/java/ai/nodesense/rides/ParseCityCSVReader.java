package ai.nodesense.rides;

import java.io.FileReader;
import java.util.Arrays;

import com.opencsv.CSVReader;



public class ParseCityCSVReader {
    public static void main(String args[]) throws  Exception {
        CSVReader reader = new CSVReader(new FileReader("/Users/krish/Downloads/Taxi_Trips.csv"),
                                ',' , '"' , 1);

        //Read CSV line by line and use the string array as you want
        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            if (nextLine != null) {
                //Verifying the read data here
                System.out.println(Arrays.toString(nextLine));
                Thread.sleep(1000);
            }
        }
    }
}
