import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class hw8 {

    private static int getRandomNumberInRange(int min, int max) {
        Random r = new Random();
        return r.ints(min, (max + 1)).findFirst().getAsInt();
    }

    public static void main(String[] args) throws SQLException {

        /*****************
         * create table testdata
         * (
         * pk  integer primary key,
         * ht  integer,
         * ot  integer,
         * hund    integer,
         * ten     integer,
         * filler  varchar(255));
         *
         * select count(*) from testdata;
         *
         * create table A as select * from testdata;
         * create table B as select * from testdata;
         * create table C as select * from testdata;
         *
         * --Create Indexes
         * create table Aprime as select * from A;
         * create index idx_aprime_ht on Aprime(ht);
         * create index idx_aprime_tt on Aprime(tt);
         * create index idx_aprime_ot on Aprime(ot);
         * create index idx_aprime_hund on Aprime(hund);
         * create index idx_aprime_ten on Aprime(ten);
         *
         * create table Bprime as select * from B;
         * create index idx_bprime_ht on Bprime(ht);
         * create index idx_bprime_tt on Bprime(tt);
         * create index idx_bprime_ot on Bprime(ot);
         * create index idx_bprime_hund on Bprime(hund);
         * create index idx_bprime_ten on Bprime(ten);
         *
         * create table Cprime as select * from C;
         * create index idx_cprime_ht on Cprime(ht);
         * create index idx_cprime_tt on Cprime(tt);
         * create index idx_cprime_ot on Cprime(ot);
         * create index idx_cprime_hund on Cprime(hund);
         * create index idx_cprime_ten on Cprime(ten);
         *
         * --Update Stats
         * analyze A;
         * analyze B;
         * analyze C;
         *
         * analyze Aprime;
         * analyze Bprime;
         * analyze Cprime;
         */


        int recordCount = 5000000;


        Connection conn1 = DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres");
        Statement st1 = conn1.createStatement();
        st1.execute("DROP TABLE IF EXISTS testdata");
        st1.execute("CREATE TABLE IF NOT EXISTS testdata (" +
                "pk integer primary key," +
                "ht integer, " +
                "tt integer, " +
                "ot integer," +
                "hund integer," +
                "ten integer," +
                "filler varchar(255));");


        PreparedStatement pst1 = conn1.prepareStatement("insert into testdata values (?, ?, ?, ?, ?, ?, ?)");

        Integer[] pkArrayOrdered = new Integer[recordCount];
        for (int i = 0; i < recordCount; i++) {
            pkArrayOrdered[i] = i;
        }

        Integer[] arrayHT = new Integer[100000];
        for (int i = 0; i < 100000; i++) {
            arrayHT[i] = i;
        }

        Integer[] arrayTT = new Integer[10000];
        System.arraycopy(arrayHT, 0, arrayTT, 0, 10000);

        Integer[] arrayOT = new Integer[1000];
        System.arraycopy(arrayTT, 0, arrayOT, 0, 1000);

        Integer[] arrayHUND = new Integer[100];
        System.arraycopy(arrayOT, 0, arrayHUND, 0, 100);

        Integer[] arrayTEN = new Integer[10];
        System.arraycopy(arrayHUND, 0, arrayTEN, 0, 10);

        Collections.shuffle(Arrays.asList(arrayHT));
        Collections.shuffle(Arrays.asList(arrayTT));
        Collections.shuffle(Arrays.asList(arrayTT));
        Collections.shuffle(Arrays.asList(arrayOT));
        Collections.shuffle(Arrays.asList(arrayOT));
        Collections.shuffle(Arrays.asList(arrayOT));
        Collections.shuffle(Arrays.asList(arrayHUND));
        Collections.shuffle(Arrays.asList(arrayHUND));
        Collections.shuffle(Arrays.asList(arrayHUND));
        Collections.shuffle(Arrays.asList(arrayTEN));
        Collections.shuffle(Arrays.asList(arrayTEN));
        Collections.shuffle(Arrays.asList(arrayTEN));
        Collections.shuffle(Arrays.asList(arrayTEN));

        String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        char[] alphaArray = alpha.toCharArray();

        long startTime1 = System.nanoTime();

        for (int i = 0; i < recordCount; i++) {
            String fillerValue = "";

            for(int j=0; j < getRandomNumberInRange(200, 250); j++){
                fillerValue += alphaArray[getRandomNumberInRange(0,51)];
            }

            pst1.clearParameters();
            pst1.setInt(1, pkArrayOrdered[i]);
            pst1.setInt(2, arrayHT[i % 100000]);
            pst1.setInt(3, arrayTT[i % 10000]);
            pst1.setInt(4, arrayOT[i % 1000]);
            pst1.setInt(5, arrayHUND[i % 100]);
            pst1.setInt(6, arrayTEN[i % 10]);
            pst1.setString(7, fillerValue);

            pst1.addBatch();
            if ((i + 1) % 50000 == 0) {
                pst1.executeBatch();
                pst1.clearBatch();
            }
        }

        conn1.close();



        long endTime1 = System.nanoTime();
        long totalTime1 = endTime1 - startTime1;
        double timeInSeconds = (double)totalTime1/1000000000.00;
        System.out.println("Records inserted in " + timeInSeconds + " seconds.");
    }
}

