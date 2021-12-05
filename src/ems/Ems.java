package ems;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Ems {

    public static void main(String[] args) throws Exception {
        genFile(100);
    }

    static void genFile(int bytes) throws IOException {
        File file = new File("data/small.txt");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);
        int bytesWritten = 0;
        Random random = new Random();
        while (true) {
            bw.write(String.valueOf(random.nextInt(99999)));
            bw.write(",");
            bytesWritten += 6;
            if (bytesWritten >= bytes) {
                break;
            }
        }
        bw.close();
    }

}
