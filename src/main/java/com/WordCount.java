package com;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/*
    This is a multi threaded java program to calculate word count in the given input txt file.

    Steps to run
    1) Make sure java 1.7 or above is installed on the machine
    2) Install maven 3.3.9
    3) Copy this package to EC2 machine
    4) cd WordCount
    5) mvn clean install
    6) mvn exec:java -Dexec.mainClass="com.WordCount" -Dexec.args="/home/ec2-user/input.txt 1"
       (program takes 2 arguments first one is the input file path and second one is number of threads. You can alter
       thread count and check values)
    7) Once program is executed, have a look at output.txt for result
 */



/**
 * Created by HarshPatil on 11/21/16.
 */
public class WordCount {

    String outputFile = "/home/ec2-user/WordCount/outputFile.txt";
    HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();
    ReadInputDataRunnable[] readInputDataRunnable;

    public static void main(String []args) throws Exception {

        String inputFile = args[0];
        int numberOfThreads = Integer.parseInt(args[1]);

        System.out.println("Started executing");
        long startTime = System.currentTimeMillis();
        WordCount wordCount = new WordCount();

        Thread[] thread = new Thread[numberOfThreads];
        wordCount.readInputDataRunnable = new ReadInputDataRunnable[numberOfThreads];

        LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(new File(inputFile)));
        lineNumberReader.skip(Long.MAX_VALUE);
        int totalNumberOfLines = lineNumberReader.getLineNumber() + 1;
        lineNumberReader.close();

        int offset = 0;
        int linesToBeReadPerThread;
        if((totalNumberOfLines%numberOfThreads) == 0){
            linesToBeReadPerThread = totalNumberOfLines/numberOfThreads;
        } else {
            linesToBeReadPerThread = totalNumberOfLines/numberOfThreads + 1;
        }

        for(int i=0; i<numberOfThreads; i++){

            wordCount.readInputDataRunnable[i] = new ReadInputDataRunnable(offset, offset+linesToBeReadPerThread, inputFile);
            offset = offset + linesToBeReadPerThread;
            thread[i] = new Thread(wordCount.readInputDataRunnable[i], "TID"+i);
            thread[i].start();
        }

        for(int i=0; i<numberOfThreads; i++){

            thread[i].join();
        }

        for(int i =0;i<numberOfThreads;i++){

            HashMap<String,Integer> partialMap = wordCount.readInputDataRunnable[i].partiallyCalculatedMap;

            Iterator it = partialMap.entrySet().iterator();
            while(it.hasNext()){

                Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) it.next();
                String key = pair.getKey();
                Integer value = pair.getValue();

                if(wordCount.wordCountMap.containsKey(key)){

                    int computedValue = wordCount.wordCountMap.get(key);
                    computedValue = computedValue + value;
                    wordCount.wordCountMap.put(key, computedValue);
                }else {
                    wordCount.wordCountMap.put(key,value);
                }
            }
        }

        wordCount.writeOutputToAFile();

        long endTime = System.currentTimeMillis();
        System.out.println("Total Threads = "+numberOfThreads + "\nTime Taken in Milliseconds = " + (endTime-startTime));

        System.out.println("End of program");

    }

    public void writeOutputToAFile() throws Exception {

        FileWriter fileWriter = new FileWriter(outputFile, false);
        Iterator it = wordCountMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)it.next();
            fileWriter.write(pair.getKey() + " :: " + pair.getValue() +"\n");
        }
        fileWriter.flush();
        fileWriter.close();
    }

    public static class ReadInputDataRunnable implements Runnable {

        HashMap<String, Integer> partiallyCalculatedMap = new HashMap<String, Integer>();
        int startRow;
        int lastRow;
        String inputFile;

        public ReadInputDataRunnable(int startRow, int lastRow, String inputFile){

            this.startRow = startRow;
            this.lastRow = lastRow;
            this.inputFile = inputFile;
        }

        public synchronized void run() {

            try {
                System.out.println("Thread "+Thread.currentThread().getId());
                readDataFromFileAndCountWords(startRow, lastRow, partiallyCalculatedMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public synchronized void readDataFromFileAndCountWords(int startRow, int lastRow, HashMap<String, Integer> partiallyCalculatedMap) throws Exception {

            int lineNumber = 0;
            String lineFromInputFile = "";
            BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));

            while (lineNumber != startRow && (lineFromInputFile = bufferedReader.readLine()) != null){
                lineNumber++;
            }

            while (lineNumber != lastRow && (lineFromInputFile = bufferedReader.readLine()) != null ) {

                lineNumber++;
                String[] wordInALine = lineFromInputFile.split("\\s+");

                for(int i=0; i<wordInALine.length; i++){

                    wordInALine[i] = wordInALine[i].replaceAll("[^a-zA-Z]", "");
                    if(!partiallyCalculatedMap.containsKey(wordInALine[i])){
                        partiallyCalculatedMap.put(wordInALine[i], 1);
                    }else{
                        int count = partiallyCalculatedMap.get(wordInALine[i]);
                        count++;
                        partiallyCalculatedMap.put(wordInALine[i], count);
                    }
                }
            }
        }
    }
}
