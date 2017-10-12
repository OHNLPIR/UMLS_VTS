package edu.mayo.bsi.umlsvts.vocabutils;

import org.sqlite.SQLiteConfig;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Contains Utility Methods Pertaining to Operations with SNOMED Clinical Term Codes <br>
 * <br>
 * This class and all static accesses are Thread-Safe
 */
public class SNOMEDCTUtils {
    // Store relations as a flat map (as opposed to code pairs) for fast lookups up and down multiple nodes in a tree
    private static Map<String, Collection<String>> PARENTS_TO_CHILD_MAP = new HashMap<>();

    private static final AtomicBoolean LCK = new AtomicBoolean(true);
    private static final AtomicBoolean REL = new AtomicBoolean(false);

    static { // Static initializers are thread-safe by default, we do not need to worry about locking
        init();
        String vocabPath = System.getProperty("vocab.src.dir");
        if (!vocabPath.endsWith("/")) {
            vocabPath = vocabPath + "/";
        }
        String connURL = "jdbc:sqlite:" + vocabPath + "SNOMEDCT_US/SNOMEDCT_US.sqlite";
        try {
            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(true);
            System.out.println("Importing SNOMED Vocabulary");
            Connection snomedConn = DriverManager.getConnection(connURL, config.toProperties());
            System.out.println("Done");
            Statement s = snomedConn.createStatement();
            ResultSet rs = s.executeQuery("SELECT sourceId, destinationId FROM Relationship WHERE typeId=116680003"); // source = child, destination = parent
            HashMap<String, Collection<String>> tempDefs = new HashMap<>();
            while (rs.next()) {
                tempDefs.computeIfAbsent(rs.getString("destinationId"), k -> new HashSet<>()).add(rs.getString("sourceId"));
            }
            for (Map.Entry<String, Collection<String>> e : tempDefs.entrySet()) {
                String parentID = e.getKey();
                PARENTS_TO_CHILD_MAP.put(parentID, generateChildrenCodes(parentID, tempDefs, new HashSet<>()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

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
            // - SNOMEDCT for Hierarchy Checks
            File snomedDir = new File(new File(vocabPath), "SNOMEDCT_US");
            File snomedVocab = new File(snomedDir, "SNOMEDCT_US.sqlite");
            try {
                Class.forName("org.sqlite.JDBC"); // Force load the driver class
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Could not find SQLITE JDBC Driver in ClassPath!");
            }
            if (!snomedDir.exists() && !snomedDir.mkdirs()) {
                throw new RuntimeException("Could not write to working directory!");
            }
            if (!snomedVocab.exists()) {
                try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
                    if (conn != null) {
                        conn.getMetaData(); // Trigger a db creation
                        // - Load CSVs (really tab delimited in default format)
                        File[] tsvList = snomedDir.listFiles();
                        if (tsvList == null) {
                            throw new RuntimeException("Tab separated definition files for SNOMEDCT database generation missing!");
                        }
                        for (File tsv : tsvList) {
                            if (!tsv.getName().endsWith(".txt")) {
                                continue;
                            }
                            BufferedReader reader = new BufferedReader(new FileReader(tsv));
                            String next;
                            // - Table Definition
                            next = reader.readLine();
                            String[] parsed = next.split("\t");
                            String tableName = tsv.getName().substring(0, tsv.getName().length() - 4).split("_")[1];
                            StringBuilder tableBuilder = new StringBuilder("CREATE TABLE ")
                                    .append(tableName)
                                    .append(" (");
                            StringBuilder indexBuilder = new StringBuilder("CREATE INDEX ")
                                    .append(tableName)
                                    .append("_IDX ON ")
                                    .append(tableName)
                                    .append(" (");
                            boolean flag = false;
                            for (String s : parsed) {
                                if (flag) {
                                    tableBuilder.append(",");
                                    indexBuilder.append(",");
                                } else {
                                    flag = true;
                                }
                                tableBuilder.append(s);
                                tableBuilder.append(" VARCHAR(255)");
                                indexBuilder.append(s);
                            }
                            indexBuilder.append(");");
                            tableBuilder.append(");");
                            System.out.println("Creating Table " + tableName);
                            conn.createStatement().execute(tableBuilder.toString());
                            // - Index for performance
                            conn.createStatement().executeUpdate(indexBuilder.toString());
                            // - Insert values into table
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

                        // - Write In-Memory DB To File
                        System.out.print("Saving Database to Disk...");
                        conn.createStatement().execute("backup to " + vocabPath + "SNOMEDCT_US/SNOMEDCT_US.sqlite");
                        System.out.println("Done");
                        synchronized (REL) {
                            REL.set(true);
                            REL.notifyAll();
                        }
                    }
                } catch (SQLException | IOException e) {
                    throw new IllegalStateException("Error occured during database initialization", e);
                }
            }
        } else {
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

    // Recursively generates a set containing all possible child codes of a given concept code
    private static Set<String> generateChildrenCodes(String code, Map<String, Collection<String>> defs, Set<String> alreadyChecked) {
        HashSet<String> ret = new HashSet<>();
        alreadyChecked.add(code);
        for (String s : defs.getOrDefault(code, new HashSet<>())) {
            if (alreadyChecked.contains(s)) {
                continue;
            }
            ret.addAll(generateChildrenCodes(s, defs, alreadyChecked));
            ret.add(s);
        }
        return ret;
    }

    /**
     * Checks if a given child concept is a subclass of the given parent concept
     *
     * @param childCode  The child concept to check
     * @param parentCode The parent concept to check
     * @return True if parentCode is the same as or a parent of childCode
     */
    public static boolean isChild(String childCode, String parentCode) {
        return PARENTS_TO_CHILD_MAP.getOrDefault(parentCode, new HashSet<>()).contains(childCode) || childCode.equals(parentCode);
    }
}
