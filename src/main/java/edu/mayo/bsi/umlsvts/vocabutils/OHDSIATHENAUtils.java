package edu.mayo.bsi.umlsvts.vocabutils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utilities for handling OHDSI ATHENA (http://athena.ohdsi.org/) vocabularies.
 */
public class OHDSIATHENAUtils {
    private static final AtomicBoolean LCK = new AtomicBoolean(true);
    private static final AtomicBoolean REL = new AtomicBoolean(false);

    /**
     * Verifies integrity of databases involved in this class, and/or creates them from csv/tsv source
     * files if available. Thread-safe
     *
     * @throws IllegalStateException if an error occurs during database initialization
     */
    private static void init() {
        if (LCK.getAndSet(false)) { // Acquired lock
            String vocabPath = System.getProperty("vocab.src.dir");
            if (vocabPath == null) {
                System.out.println("Please provide the full path to the directory containing vocabulary definitions using" +
                        "-Dvocab.src.dir");
                throw new IllegalStateException("-Dvocab.src.dir not set");
            }
            if (!vocabPath.endsWith("/")) {
                vocabPath = vocabPath + "/";
            }
            // - Check for an OHDSI Folder
            File vocabDefFolder = new File(new File(vocabPath),"OHDSI");
            if (!vocabDefFolder.mkdirs()) {
                System.out.println("Could not create OHDSI working directory!");
            }
            File[] files = vocabDefFolder.listFiles();
            if (files == null) {
                throw new IllegalStateException("Could not access the vocabulary definition folder");
            }
            if (!vocabDefFolder.exists() || files.length == 0) {
                System.out.println("OHDSI vocabularies not present");
                System.out.println("Please download from http://athena.ohdsi.org/ snd place in the \"OHDSI\" folder");
                throw new IllegalStateException("Vocabulary source and database not present for generation");
            }
            // - Load OHDSI Vocabularies into SQLite if Necessary
            File OHDSIVocab = new File(vocabDefFolder, "ATHENA.sqlite");
            try {
                Class.forName("org.sqlite.JDBC"); // Force load the driver class
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Could not find SQLITE JDBC Driver in Classpath!");
            }
            if (!OHDSIVocab.exists()) {
                try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
                    if (conn != null) {
                        conn.getMetaData(); // Trigger a db creation
                        // - Load CSVs (really tab delimited in default format)
                        File[] fileLis = vocabDefFolder.listFiles();
                        if (fileLis == null) {
                            throw new IllegalStateException("Could not access the vocabulary definition folder");
                        }
                        for (File csv : fileLis) {
                            if (!csv.getName().endsWith(".csv")) {
                                continue;
                            }
                            BufferedReader reader = new BufferedReader(new FileReader(csv));
                            String next;
                            // - Table Definition
                            next = reader.readLine();
                            String[] parsed = next.split("\t");
                            String tableName = csv.getName().substring(0, csv.getName().length() - 4);
                            StringBuilder tableBuilder = new StringBuilder("CREATE TABLE ")
                                    .append(tableName)
                                    .append(" (");
                            boolean flag = false;
                            for (String s : parsed) {
                                if (flag) {
                                    tableBuilder.append(",");
                                } else {
                                    flag = true;
                                }
                                tableBuilder.append(s);
                                tableBuilder.append(" VARCHAR(255)");
                            }
                            tableBuilder.append(");");
                            System.out.println("Creating Table " + tableName);
                            conn.createStatement().execute(tableBuilder.toString());
                            StringBuilder insertStatementBuilder = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
                            flag = false;
                            for (String ignored : parsed) {
                                if (flag) {
                                    insertStatementBuilder.append(",");
                                } else {
                                    flag = true;
                                }
                                insertStatementBuilder.append("?");
                            }
                            insertStatementBuilder.append(");");
                            // - Insert values into table
                            PreparedStatement ps = conn.prepareStatement(insertStatementBuilder.toString());
                            System.out.println("Loading Table " + tableName);
                            int counter = 0;
                            while ((next = reader.readLine()) != null && next.length() > 1) {
                                if (counter > 0 && counter % 500000 == 0) {
                                    System.out.print("Inserting " + counter + " Elements...");
                                    ps.executeBatch();
                                    ps.clearBatch();
                                    System.out.println("Done");
                                    counter = 0;
                                }
                                String[] values = next.trim().split("\t");
                                for (int j = 0; j < values.length; j++) {
                                    ps.setString(j + 1, values[j]);
                                }
                                ps.addBatch();
                                counter++;
                            }
                            System.out.print("Inserting " + counter + " Elements...");
                            ps.executeBatch();
                            System.out.println("Done");
                        }
                        // - Index for performance
                        conn.createStatement().executeUpdate("CREATE INDEX CONCEPT_INDEX ON CONCEPT (CONCEPT_CODE, CONCEPT_NAME, CONCEPT_ID)");
                        // - Write In-Memory DB To File
                        System.out.print("Saving Database to Disk...");
                        conn.createStatement().execute("backup to " + vocabPath + "OHDSI/ATHENA.sqlite");
                        System.out.println("Done");
                        synchronized (REL) {
                            REL.set(true);
                            REL.notifyAll();
                        }
                    }
                } catch (SQLException | IOException e) {
                    e.printStackTrace();
                }
            }
        } else { // Did not acquire lock, spin until initialization is completed
            synchronized (REL) {
                while (!REL.get()) {
                    try {
                        REL.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
