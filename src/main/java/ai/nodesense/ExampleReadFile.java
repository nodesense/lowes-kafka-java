package ai.nodesense;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ExampleReadFile {
    public static void main(String[] args) throws IOException {
        try {
            File f = new File ("/Users/krish/Downloads/Taxi_Trips.csv");
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = "";
            System.out.println("Read file");

            while ( (line = br.readLine()) != null) {
                System.out.println(line);
            }

        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
