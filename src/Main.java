/**************************************
 * Data Engineering - Fall 2018
 * University of Texas at Austin
 * Prof Miranker
 * Homework 6
 * Patrick Sigourney
 **************************************/

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;


public class Main {
    private static int getRandomNumberInRange(int min, int max) {
        Random r = new Random();
        return r.ints(min, (max + 1)).findFirst().getAsInt();
    }

    private static double queryTable(int colA, int colB) throws SQLException{
        Connection conn2 = DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres");
        Statement st2 = conn2.createStatement();
        String baseQuery = "SELECT * FROM benchmark";
        if(colA > 0 || colB > 0){
            baseQuery += " WHERE ";
        }
        if(colA > 0){
            baseQuery += "columnA = " + Integer.toString(colA);
            if(colB > 0){
                baseQuery += " AND " ;
            }
        }
        if(colB > 0){
            baseQuery += "columnB = " + Integer.toString(colB);
        }

        long startTime1 = System.nanoTime();

        st2.execute(baseQuery);

        long endTime1 = System.nanoTime();
        long totalTime1 = endTime1 - startTime1;
        return (double)totalTime1/1000000000.00;
    }

    private static double insertRecords(int recordCount, boolean random, boolean indexA, boolean indexB) throws SQLException{
        Connection conn1 = DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres");
        Statement st1 = conn1.createStatement();
        st1.execute("DROP TABLE IF EXISTS testdata");
        st1.execute("CREATE TABLE IF NOT EXISTS testdata (pk integer primary key,ht integer,ot integer,hund integer,ten integer,filler varchar(255));");


        PreparedStatement pst1 = conn1.prepareStatement("insert into testdata values (?, ?, ?, ?, ?, ?)");

        Integer[] pkArrayOrdered = new Integer[recordCount];
        for(int i = 0; i < recordCount; i++){
            pkArrayOrdered[i] = i+1;
        }

        Integer[] arrayHT = new Integer[100000];
        for(int i = 0; i < 100000; i++){
            arrayHT[i] = i+1;
        }

        Integer[] arrayTT = new Integer[10000];
        System.arraycopy(arrayHT, 0, arrayTT, 0, 9999);

        Integer[] arrayOT = new Integer[1000];
        System.arraycopy(arrayTT, 0, arrayOT, 0, 999);

        Integer[] arrayHUND = new Integer[100];
        System.arraycopy(arrayOT, 0, arrayHUND, 0, 99);

        Integer[] arrayTEN = new Integer[10];
        System.arraycopy(arrayHUND, 0, arrayTEN, 0, 9);


        Collections.shuffle(Arrays.asList(arrayHT));
        Collections.shuffle(Arrays.asList(arrayTT));
        Collections.shuffle(Arrays.asList(arrayOT));
        Collections.shuffle(Arrays.asList(arrayHUND));
        Collections.shuffle(Arrays.asList(arrayTEN));

        String[] fillerArray = new String[5];
        fillerArray[0] = "dasloghwg";
        fillerArray[1] = "dahg";
        fillerArray[2] = "adujghadglhadgljhgwljvhbworuvhbro";
        fillerArray[3] = "jhgwjuvnwrlvnrewlhglsavnklmvnklwrjgnwrvhrwolrfjbewrogbvrkb";
        fillerArray[4] = "hdwsjvhbwvbhowrvlknqwrbpoirj3vm423iorenvoermrwvnwr";

        long startTime1 = System.nanoTime();

        for(int i = 0; i < recordCount; i++){
            pst1.clearParameters();
            pst1.setInt(1, pkArrayOrdered[i]);
            pst1.setInt(2, arrayHT[i%100000]);
            pst1.setInt(3, arrayTT[i%10000]);
            pst1.setInt(4, arrayOT[i%1000]);
            pst1.setInt(5, arrayHUND[i%100]);
            pst1.setInt(6, arrayTEN[i%10]);
            pst1.setString(7, fillerArray[getRandomNumberInRange(0,4)]);

            pst1.addBatch();
            if((i+1)%50000 == 0){
                pst1.executeBatch();
                pst1.clearBatch();
            }
        }

        conn1.close();

        long endTime1 = System.nanoTime();
        long totalTime1 = endTime1 - startTime1;
        return (double)totalTime1/1000000000.00;
    }


    public static void main(String[] args) {
        int n = 5000000;
        try {
            int[] intArray = new int[] {123456, 234567, 345678, 456789, 567890, 987654, 876543, 765432, 10, 500};



            double orderedSeconds = insertRecords(n, false, true, true);
            System.out.println("Seconds to insert " + n + " ordered records: " + orderedSeconds);
            double query1Total = 0.0;
            for(int i = 0; i < intArray.length; i++){
                query1Total += queryTable(intArray[i], 0);
            }
            double query1Avg = query1Total/intArray.length;
            System.out.println("Seconds to query a=__ ordered records with B indexes: " + query1Avg);


            double query2Total = 0.0;
            for(int i = 0; i < intArray.length; i++){
                query2Total += queryTable(0, intArray[i]);
            }
            double query2Avg = query2Total/intArray.length;
            System.out.println("Seconds to query b=__ ordered records with B indexes: " + query2Avg);



            double query3Total = 0.0;
            for(int i = 0; i < intArray.length; i++){
                query3Total += queryTable(intArray[i], intArray[i]);
            }
            double query3Avg = query3Total/intArray.length;
            System.out.println("Seconds to query a=__ and b=__ ordered records with B indexes: " + query3Avg);



/********************************************************************************************************************/
/*     RANDOM INSERTS
*/

            double randomSeconds = insertRecords(n, true, true, true);
            System.out.println("Seconds to insert " + n + " random records: " + randomSeconds);

             query1Total = 0.0;
            for(int i = 0; i < intArray.length; i++){
                query1Total += queryTable(intArray[i], 0);
            }
             query1Avg = query1Total/intArray.length;
            System.out.println("Seconds to query a=__ random records with B indexes: " + query1Avg);


             query2Total = 0.0;
            for(int i = 0; i < intArray.length; i++){
                query2Total += queryTable(0, intArray[i]);
            }
             query2Avg = query2Total/intArray.length;
            System.out.println("Seconds to query b=__  random records with B indexes: " + query2Avg);



             query3Total = 0.0;
            for(int i = 0; i < intArray.length; i++){
                query3Total += queryTable(intArray[i], intArray[i]);
            }
             query3Avg = query3Total/intArray.length;
            System.out.println("Seconds to query a=__ AND b=__ random records with B indexes: " + query3Avg);

            System.out.println("Query time average over " + intArray.length + " passes");

        }catch (SQLException e) {System.out.println("SQLException: " + e);}
    }
}
