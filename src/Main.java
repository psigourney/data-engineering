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


    public static void main(String[] args) {
        try {

            Connection conn1 = DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres");
            Statement st1 = conn1.createStatement();
            st1.execute("DROP TABLE IF EXISTS benchmark");
            st1.execute("CREATE TABLE IF NOT EXISTS benchmark (theKey numeric PRIMARY KEY, columnA numeric, columnB numeric, filler char(247))");

            PreparedStatement pst1 = conn1.prepareStatement("insert into benchmark values (?, ?, ?, ?)");

            int n = 5000000; // number of records to insert

            Integer[] pkArrayOrdered = new Integer[n];
            for(int i = 0; i < n; i++){
                pkArrayOrdered[i] = i+1;
            }

            Integer[] pkArrayRandom = new Integer[n];
            for(int i = 0; i < n; i++){
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

            for(int i = 0; i < n; i++){
                int rand1 = getRandomNumberInRange(0,4);

                pst1.clearParameters();

                pst1.setInt(1, pkArrayOrdered[i]);  //theKey
                pst1.setInt(2, pkArrayRandom[i]); //columnA
                pst1.setInt(3, pkArrayRandom[n-i]); //columnB
                pst1.setString(4, fillerArray[rand1]); //filler

                pst1.addBatch();
                if(i%50000 == 0){
                    pst1.executeBatch();
                    pst1.clearBatch();
                }

            }

            conn1.close();

            long endTime1 = System.nanoTime();
            long totalTime1 = endTime1 - startTime1;
            double totalSeconds1 = (double)totalTime1/1000000000.00;
            System.out.println("Total time taken for sorted insert of " + n + " records is " + totalSeconds1 + " seconds");


/******************************************************************************************************************/


            Connection conn2 = DriverManager.getConnection("jdbc:postgresql://localhost/", "postgres", "postgres");
            Statement st2 = conn2.createStatement();
            st2.execute("DROP TABLE IF EXISTS benchmark");
            st2.execute("CREATE TABLE IF NOT EXISTS benchmark (theKey numeric PRIMARY KEY, columnA numeric, columnB numeric, filler char(247))");

            PreparedStatement pst2 = conn2.prepareStatement("insert into benchmark values (?, ?, ?, ?)");

            long startTime2 = System.nanoTime();

            for(int i = 0; i < n; i++){
                int rand1 = getRandomNumberInRange(0,4);

                pst2.clearParameters();

                pst2.setInt(1, pkArrayRandom[i]);  //theKey
                pst2.setInt(2, pkArrayRandom[i]); //columnA
                pst2.setInt(3, pkArrayRandom[n-i]); //columnB
                pst2.setString(4, fillerArray[rand1]); //filler

                pst2.addBatch();
                if(i%50000 == 0){
                    pst2.executeBatch();
                    pst2.clearBatch();
                }

            }

            conn2.close();

            long endTime2 = System.nanoTime();
            long totalTime2 = endTime2 - startTime2;
            double totalSeconds2 = (double)totalTime2/1000000000.00;
            System.out.println("Total time taken for random insert of " + n + " records is " + totalSeconds2 + " seconds");

        }catch(SQLException e){System.out.println("SQL Exception: " + e); System.exit(1);}

    }
}
