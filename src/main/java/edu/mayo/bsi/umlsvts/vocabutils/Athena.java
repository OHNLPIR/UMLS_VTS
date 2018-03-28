package edu.mayo.bsi.umlsvts.vocabutils;

import com.mchange.v2.c3p0.DataSources;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * An Athena Accessor Interface, All Methods are Thread-Safe
 */
@SuppressWarnings("WeakerAccess unused")
public class Athena {
    /**
     * An enumeration of supported ATHENA source vocabularies
     */
    public enum SourceVocabulary {
        /**
         * Current Procedural Terminology - Version 4
         */
        CPT4,
        /**
         * International Classification of Diseases, 10th Revision
         */
        ICD10,
        /**
         * International Classification of Diseases, 10th Revision, Clinical Modification
         */
        ICD10CM,
        /**
         * International Classification of Diseases, 10th Revision, Procedure Coding System
         */
        ICD10PCS,
        /**
         * International Classification of Diseases, 9th Revision, Clinical Modification
         */
        ICD9CM,
        /**
         * International Classification of Diseases, 9th Revision, Clinical Modification Vol. 3 - Procedure Codes
         */
        ICD9Proc,
        /**
         * RxNorm
         */
        RXNORM("RxNorm"),
        /**
         * RxNorm Extension
         */
        RXNORM_EXT("RxNorm Extension"),
        /**
         * Health care Common Procedure Coding System
         */
        HCPCS,
        /**
         * Systematized Nomenclature of Medicine - Clinical Terms (SNOMED CT)
         */
        SNOMED;

        private final String name;

        SourceVocabulary() {
            this.name = this.name();
        }


        SourceVocabulary(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * A lock on database initialization: a value of true indicates no thread has attempted to initialize the database yet
     */
    private static final AtomicBoolean LCK = new AtomicBoolean(true);
    /**
     * A lock on database initialization completion, a value of false indicates that the Athena DB has not yet completed initialization
     */
    private static final AtomicBoolean REL = new AtomicBoolean(false);
    /**
     * Logger instance
     */
    private static final Logger LOGGER = Logger.getLogger("OHDSI Athena Vocabulary Service");
    /**
     * JDBC Connection Pool
     */
    private static DataSource JDBC_DATA_SOURCE;

    static {
        // Spin up database creation in a new thread
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                initDB();
            } catch (Throwable e) {
                // Hard exit on error to prevent issue being missed
                e.printStackTrace();
                System.exit(1);
            }
        });
    }

    /**
     * Actually creates the database and relevant connections or causes the thread to wait if db creation is in progress
     */
    private static void initDB() {
        if (LCK.getAndSet(false)) { // Acquired lock
            String vocabPath = System.getProperty("vocab.src.dir");
            if (vocabPath == null) {
                vocabPath = System.getProperty("user.dir");
                LOGGER.info("-Dvocab.src.dir not set, defaulting to current working directory: " + vocabPath);
            }
            if (!vocabPath.endsWith("/")) {
                vocabPath = vocabPath + "/";
            }
            // - Check for an OHDSI Folder
            File vocabDefFolder = new File(new File(vocabPath), "OHDSI");
            if (!vocabDefFolder.exists() && !vocabDefFolder.mkdirs()) {
                LOGGER.severe("OHDSI working directory could not be created at " + vocabDefFolder.getAbsolutePath());
                throw new IllegalStateException("Could not create OHDSI working directory!");
            }
            File[] files = vocabDefFolder.listFiles();
            if (files == null) {
                LOGGER.severe("OHDSI definitions could not be accessed at " + vocabDefFolder.getAbsolutePath());
                throw new IllegalStateException("Could not access the vocabulary definition folder");
            }
            if (!vocabDefFolder.exists() || files.length == 0) {
                LOGGER.severe("OHDSI vocabularies not present");
                LOGGER.severe("Please download from http://athena.ohdsi.org/ snd place in the \"OHDSI\" folder");
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
                            LOGGER.info("Creating Table " + tableName);
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
                            LOGGER.info("Loading Table " + tableName);
                            int counter = 0;
                            while ((next = reader.readLine()) != null && next.length() > 1) {
                                if (counter > 0 && counter % 500000 == 0) {
                                    LOGGER.info("Inserting " + counter + " Elements...");
                                    ps.executeBatch();
                                    ps.clearBatch();
                                    LOGGER.info("Done");
                                    counter = 0;
                                }
                                String[] values = next.trim().split("\t");
                                for (int j = 0; j < values.length; j++) {
                                    ps.setString(j + 1, values[j]);
                                }
                                ps.addBatch();
                                counter++;
                            }
                            LOGGER.info("Inserting " + counter + " Elements...");
                            ps.executeBatch();
                            LOGGER.info("Done");
                        }
                        // - Index for performance
                        conn.createStatement().executeUpdate("CREATE INDEX CONCEPT_INDEX ON CONCEPT (CONCEPT_CODE, CONCEPT_NAME, CONCEPT_ID)");
                        // - Write In-Memory DB To File
                        LOGGER.info("Saving Database to Disk...");
                        conn.createStatement().execute("backup to \"" + vocabPath.replace('\\', '/') + "OHDSI/ATHENA.sqlite\"");
                        LOGGER.info("Done");
                    }
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                SQLiteConfig config = new SQLiteConfig();
                config.setReadOnly(true);
                LOGGER.info("Initializing OHDSI Vocabulary Definitions");
                String url = "jdbc:sqlite:" + vocabPath.replace('\\', '/') + "OHDSI/ATHENA.sqlite";
                SQLiteDataSource sqLiteDataSource = new SQLiteDataSource(config);
                sqLiteDataSource.setUrl(url);
                JDBC_DATA_SOURCE = DataSources.pooledDataSource(sqLiteDataSource, 180); // TODO configurable
                LOGGER.info("Done");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            synchronized (REL) {
                REL.set(true);
                REL.notifyAll();
            }
        } else if (!REL.get()){ // Check init state, if not init, did not acquire lock, spin until initialization is completed
            synchronized (REL) {
                while (!REL.get()) {
                    try {
                        REL.wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Gets the equivalent OHDSI concept ID for the provided code and source vocabulary
     * @param code The code to lookup
     * @param vocabulary The vocabulary defining the parameter code
     * @return The corresponding OHDSI concept ID, or -99999 if not found
     * @throws SQLException if an error occurs during lookup
     */
    public static int getOHDSIConceptIDforSourceVocabCode(String code, SourceVocabulary vocabulary) throws SQLException {
        return getOHDSIConceptIDforSourceVocabCode(code, vocabulary.getName());
    }

    /**
     * Gets the equivalent OHDSI concept ID for the provided code and source vocabulary
     * @param code The code to lookup
     * @param vocabulary The vocabulary name of the vocabulary defining the parameter code
     * @return The corresponding OHDSI concept ID, or -99999 if not found
     * @throws SQLException if an error occurs during lookup
     */
    public static int getOHDSIConceptIDforSourceVocabCode(String code, String vocabulary) throws SQLException {
        initDB();
        Connection conn = JDBC_DATA_SOURCE
                .getConnection();
        PreparedStatement CONCEPT_LOOKUP_STATEMENT = conn
                .prepareStatement("SELECT CONCEPT_ID FROM CONCEPT WHERE VOCABULARY_ID=? AND CONCEPT_CODE=?");
        CONCEPT_LOOKUP_STATEMENT.setString(1, code);
        CONCEPT_LOOKUP_STATEMENT.setString(2, vocabulary);
        ResultSet rs = CONCEPT_LOOKUP_STATEMENT.executeQuery();
        if (rs.next()) {
            int ret = Integer.valueOf(rs.getString("CONCEPT_ID"));
            conn.close();
            return ret;
        } else {
            conn.close();
            return -99999;
        }
    }

    /**
     * Gets the equivalent OHDSI concept ID for the provided concept name and source vocabulary
     * @param name The name to lookup
     * @param vocabulary The vocabulary of the vocabulary defining the parameter code
     * @return The corresponding OHDSI concept ID, or -99999 if not found
     * @throws SQLException if an error occurs during lookup
     */
    public static int getOHDSIConceptIDforSourceVocabName(String name, SourceVocabulary vocabulary) throws SQLException {
        return getOHDSIConceptIDforSourceVocabName(name, vocabulary.getName());
    }

    /**
     * Gets the equivalent OHDSI concept ID for the provided concept name and source vocabulary
     * @param name The name to lookup
     * @param vocabulary The vocabulary name of the vocabulary defining the parameter code
     * @return The corresponding OHDSI concept ID, or -99999 if not found
     * @throws SQLException if an error occurs during lookup
     */
    public static int getOHDSIConceptIDforSourceVocabName(String name, String vocabulary) throws SQLException {
        initDB();
        Connection conn = JDBC_DATA_SOURCE.getConnection();
        PreparedStatement CONCEPT_LOOKUP_STATEMENT = conn
                .prepareStatement("SELECT CONCEPT_ID FROM CONCEPT WHERE VOCABULARY_ID=? AND CONCEPT_NAME LIKE ?");
        CONCEPT_LOOKUP_STATEMENT.setString(1, name);
        CONCEPT_LOOKUP_STATEMENT.setString(2, vocabulary);
        ResultSet rs = CONCEPT_LOOKUP_STATEMENT.executeQuery();
        if (rs.next()) {
            int ret = Integer.valueOf(rs.getString("CONCEPT_ID"));
            conn.close();
            return ret;
        } else {
            conn.close();
            return -99999;
        }
    }
}
