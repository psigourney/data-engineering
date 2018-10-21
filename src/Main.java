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
        st1.execute("DROP TABLE IF EXISTS benchmark");
        st1.execute("CREATE TABLE IF NOT EXISTS benchmark (theKey numeric PRIMARY KEY, columnA numeric, columnB numeric, filler char(247))");
        if(indexA){
            st1.execute("CREATE INDEX idx_benchmark_colA ON benchmark(columnA)");
        }
        if(indexB){
            st1.execute("CREATE INDEX idx_benchmark_colB ON benchmark(columnB)");
        }

        PreparedStatement pst1 = conn1.prepareStatement("insert into benchmark values (?, ?, ?, ?)");

        Integer[] pkArrayOrdered = new Integer[recordCount];
        for(int i = 0; i < recordCount; i++){
            pkArrayOrdered[i] = i+1;
        }

        Integer[] pkArrayRandom = new Integer[recordCount];
        for(int i = 0; i < recordCount; i++){
            pkArrayRandom[i] = i+1;
        }
        Collections.shuffle(Arrays.asList(pkArrayRandom));

        String[] fillerArray = new String[5];
        fillerArray[0] = "dasloghwg";
        fillerArray[1] = "dahg";
        fillerArray[2] = "adujghadglhadgljhgwljvhbworuvhbro";
        fillerArray[3] = "jhgwjuvnwrlvnrewlhglsavnklmvnklwrjgnwrvhrwolrfjbewrogbvrkb";
        fillerArray[4] = "hdwsjvhbwvbhowrvlknqwrbpoirj3vm423iorenvoermrwvnwr";

        long startTime1 = System.nanoTime();

        for(int i = 0; i < recordCount; i++){
            pst1.clearParameters();

            if(random) {
                pst1.setInt(1, pkArrayRandom[i]);  //theKey
            }
            else{
                pst1.setInt(1, pkArrayOrdered[i]);  //theKey
            }
            pst1.setInt(2, pkArrayRandom[i]); //columnA
            pst1.setInt(3, pkArrayRandom[recordCount-1-i]); //columnB
            pst1.setString(4, fillerArray[getRandomNumberInRange(0,4)]); //filler

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
        int n = 1000000;
        try {
            int[] intArray = new int[] {123456, 234567, 345678, 456789, 567890, 987654, 876543, 765432, 10, 500};


            double orderedSeconds = insertRecords(n, false, true, true);
            System.out.println("Seconds to insert " + n + " ordered records: " + orderedSeconds);

            double queryTotal = 0.0;
            for(int i = 0; i < intArray.length; i++){
                queryTotal += queryTable(intArray[i], intArray[i]);
            }
            double queryAvg = queryTotal/intArray.length;
            System.out.println("Seconds to query " + n + " ordered records with two indexes: " + queryAvg);

           // double randomSeconds = insertRecords(n, true, true, true);
           // System.out.println("Seconds to insert " + n + " random records: " + randomSeconds);

        }catch (SQLException e) {System.out.println("SQLException: " + e);}
    }
}
