package edu.mayo.bsi.nlp.vts;

import com.mchange.v2.c3p0.DataSources;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class supplies functionality for operations on a UMLS dataset. <br>
 * <br>
 * All methods are thread safe
 */
public class UMLS {

    private static final AtomicBoolean LCK = new AtomicBoolean(true);
    private static final AtomicBoolean REL = new AtomicBoolean(false);
    private static DataSource JDBC_POOL;

    static {
        init();
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
                vocabPath = System.getProperty("user.dir");
                System.out.println("-Dvocab.src.dir not set, defaulting to current working directory: " + vocabPath);
            }
            if (!vocabPath.endsWith("/")) {
                vocabPath = vocabPath + "/";
            }
            File umlsDir = new File(new File(vocabPath), "UMLS");
            File umlsDB = new File(umlsDir, "UMLS.sqlite");
            File umlsRRF = new File(umlsDir, "MRCONSO.RRF");
            if (!umlsDir.exists() && !umlsDir.mkdirs()) {
                throw new IllegalStateException("Could not write to working directory!");
            }
            if (!umlsDB.exists() && !umlsRRF.exists()) {
                System.out.println("A UMLS mapping of source vocabularies is required to retrieve  " +
                        "vocabulary IDs from UMLS CUIs output from various NLP pipelines. Please put MRCONSO.RRF generated " +
                        "from a MetaMorphosys installation with information from the relevant source vocabularies or a " +
                        "UMLS.sqlite generated by this application within the \"UMLS\" Folder.");
                throw new IllegalStateException("No sources to generate UMLS DB Sources");
            }
            // -- Generate a DB from the RRF file
            if (!umlsDB.exists()) {
                String url = "jdbc:sqlite::memory:"; // Load into memory first, then write for performance
                try (Connection conn = DriverManager.getConnection(url)) {
                    System.out.print("Generating UMLS Lookup SQL Database...");
                /*
                 * Table Structure Generated from
                 * https://www.nlm.nih.gov/research/umls/implementation_resources/query_diagrams/er1.html
                 * https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/columns_data_elements.html
                 * https://metamap.nlm.nih.gov/Docs/FAQ/CodeField.pdf
                 */
                    conn.createStatement().executeUpdate("CREATE TABLE CONCEPT_MAPPINGS (" +
                            "CUI CHAR(8)," +
                            "LAT CHAR(3)," +
                            "SAB VARCHAR(40)," +
                            "CODE VARCHAR(255)," +
                            "STR VARCHAR(255)" +
                            ");"
                    );
                    BufferedReader reader = new BufferedReader(new FileReader(umlsRRF));
                    String line;
                    PreparedStatement ps = conn.prepareStatement("INSERT INTO CONCEPT_MAPPINGS VALUES (?,?,?,?,?);");
                    while ((line = reader.readLine()) != null) {
                        String[] parsed = line.split("\\|");
                        if (parsed.length < 15) {
                            continue;
                        }
                        ps.setString(1, parsed[0]);
                        ps.setString(2, parsed[1]);
                        ps.setString(3, parsed[11]);
                        ps.setString(4, parsed[13]);
                        ps.setString(5, parsed[14]);
                        ps.executeUpdate();
                    }
                    System.out.println("Done");
                    // Index for performance since we are going to be making this read only anyways
                    conn.createStatement().executeUpdate("CREATE INDEX CONCEPT_INDEX_BY_CUI ON CONCEPT_MAPPINGS (CUI, SAB)");
                    conn.createStatement().executeUpdate("CREATE INDEX CONCEPT_INDEX_BY_CODE ON CONCEPT_MAPPINGS (CODE, SAB)");

                    System.out.print("Saving Database to Disk...");
                    conn.createStatement().execute("backup to \"" + vocabPath.replaceAll("\\\\", "/") + "UMLS/UMLS.sqlite\"");
                    System.out.println("Done");
                } catch (SQLException | IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException("Could not create the UMLS database");
                }
            }
            String vocabDir = vocabPath;
            if (!vocabDir.endsWith("/")) {
                vocabDir = vocabDir + "/";
            }
            String connURL = "jdbc:sqlite:" + vocabDir + "UMLS/UMLS.sqlite";
            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(true);
            SQLiteDataSource sqLiteDataSource = new SQLiteDataSource(config);
            sqLiteDataSource.setUrl(connURL);
            try {
                JDBC_POOL = DataSources.pooledDataSource(sqLiteDataSource, 180); // TODO make this configurable
            } catch (SQLException e) {
                throw new IllegalStateException("Could not create a connection pool", e);
            }

            synchronized (REL) {
                REL.set(true);
                REL.notifyAll();
            }
        } else {
            synchronized (REL) {
                while (!REL.get()) {
                    try {
                        REL.wait(30000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Retrieves equivalent codes within the various UMLS source vocabularies corresponding to a given UMLS concept <br>
     * Results from conversion can then be further
     * manipulated with the appropriate [VOCABNAME]Utils class (e.g. {@link SNOMEDCT})<br>
     *
     * @param vocab   The vocabulary to retrieve
     * @param UMLSCui The UMLS cui to retrieve equivalent codes within the supplied source vocabulary for
     * @return A collection of source vocabulary codes corresponding to the UMLS cui supplied, can be empty
     * @throws SQLException if access to the lookup database fails
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static Collection<String> getSourceCodesForVocab(UMLSSourceVocabulary vocab, String UMLSCui) throws SQLException {
        try (Connection c = JDBC_POOL.getConnection();
             PreparedStatement getSourceCodingPS = c.prepareStatement("SELECT CODE FROM CONCEPT_MAPPINGS WHERE CUI=? AND SAB=?")){
            Collection<String> ret = new LinkedList<>();
            getSourceCodingPS.setString(1, UMLSCui);
            getSourceCodingPS.setString(2, vocab.name());
            if (getSourceCodingPS.execute()) {
                ResultSet rs = getSourceCodingPS.getResultSet();
                while (rs.next()) {
                    ret.add(rs.getString("CODE"));
                }
            }
            return ret;
        }
    }

    /**
     * Performs the inverse of {@link #getSourceCodesForVocab(UMLSSourceVocabulary, String)}. retrieves a UMLS concept
     * by a source vocabulary code, if it exists. Results from this call can be chained with
     * {@link #getSourceCodesForVocab(UMLSSourceVocabulary, String)} to perform inter-vocab conversions
     *
     * @param vocab      The source vocabulary the lookup code belongs to
     * @param sourceCode The code as defined in the source vocabulary
     * @return A collection of UMLS concept unique identifier related to the source vocabulary, or an empty list if no such cui exists
     */
    @SuppressWarnings("unused")
    public static Collection<String> getUMLSCuiForSourceVocab(UMLSSourceVocabulary vocab, String sourceCode) throws SQLException {
        try (Connection c = JDBC_POOL.getConnection();
             PreparedStatement getCuiByCodePS = c.prepareStatement("SELECT CUI FROM CONCEPT_MAPPINGS WHERE CODE=? AND SAB=?")) {
            getCuiByCodePS.setString(1, sourceCode);
            getCuiByCodePS.setString(2, vocab.name());
            Collection<String> ret = new LinkedList<>();
            if (getCuiByCodePS.execute()) {
                ResultSet rs = getCuiByCodePS.getResultSet();
                while (rs.next()) {
                    ret.add(rs.getString("CUI"));
                }
            }
            return ret;
        }
    }

    /**
     * Retrieves the preferred text for a given code within the given source vocabulary
     *
     * @param vocab         The vocabulary to use
     * @param sourceConcept The concept code within that vocabulary
     * @return A set of preferred texts within the source vocabulary for the given code, or an empty set if concept is not found
     * @throws SQLException if access to the lookup database fails
     */
    @SuppressWarnings("unused")
    public static Collection<String> getSourceTermPreferredText(UMLSSourceVocabulary vocab, String sourceConcept) throws SQLException {
        try (Connection c = JDBC_POOL.getConnection();
             PreparedStatement getSourcePreferredPS = c.prepareStatement("SELECT STR FROM CONCEPT_MAPPINGS WHERE CODE=? AND SAB=?")) {
            getSourcePreferredPS.setString(1, sourceConcept);
            getSourcePreferredPS.setString(2, vocab.name());
            HashSet<String> ret = new HashSet<>();
            if (getSourcePreferredPS.execute()) {
                ResultSet rs = getSourcePreferredPS.getResultSet();
                if (rs.next()) {
                    ret.add(rs.getString("STR"));
                }
            }
            return ret;
        }
    }

    @SuppressWarnings("unused")
    public enum UMLSSourceVocabulary {
        /**
         * SNOMED Clinical Terms - United States
         */
        SNOMEDCT_US,
        /**
         * American Medical Association - Current Procedural Terminology
         */
        CPT,
        /**
         * International Classification of Diseases, Ninth Revision, Clinical Modification
         */
        ICD9CM,
        /**
         * International Classification of Diseases, Tenth Revision, Clinical Modification
         */
        ICD10CM,
        /**
         * Medical Dictionary for Regulatory Activities
         */
        MDR,
        /**
         * RXNORM
         */
        RXNORM
    }

}