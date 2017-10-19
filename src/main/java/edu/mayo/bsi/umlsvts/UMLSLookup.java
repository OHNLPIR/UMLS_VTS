package edu.mayo.bsi.umlsvts;

import edu.mayo.bsi.umlsvts.vocabutils.SNOMEDCTUtils;
import org.sqlite.SQLiteConfig;

import java.io.*;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class supplies functionality for operations on a UMLS dataset. <br>
 * <br>
 * This converter is not thread-safe, therefore a new instance should be created for each process using {@link #newLookup()} ()}
 */
public class UMLSLookup {

    private static final AtomicBoolean LCK = new AtomicBoolean(true);
    private static final AtomicBoolean REL = new AtomicBoolean(false);
    private final PreparedStatement getSourceCodingPS;
    private final PreparedStatement getSourcePreferredPS;
    private final PreparedStatement getCuiByCodePS;

    private UMLSLookup() {
        init();
        String vocabDir = System.getProperty("vocab.src.dir");
        if (!vocabDir.endsWith("/")) {
            vocabDir = vocabDir + "/";
        }
        String connURL = "jdbc:sqlite:" + vocabDir + "UMLS/UMLS.sqlite";
        try {
            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(true);
            System.out.print("Importing UMLS Vocabulary Mappings...");
            Connection ohdsiDBConn = DriverManager.getConnection(connURL, config.toProperties());
            System.out.println("Done");
            getSourceCodingPS = ohdsiDBConn.prepareStatement("SELECT CODE FROM CONCEPT_MAPPINGS WHERE CUI=? AND SAB=? AND LAT=?");
            getCuiByCodePS = ohdsiDBConn.prepareStatement("SELECT CUI FROM CONCEPT_MAPPINGS WHERE CODE=? AND SAB=? AND LAT=?");
            getSourcePreferredPS = ohdsiDBConn.prepareStatement("SELECT STR FROM CONCEPT_MAPPINGS WHERE SAB=? AND CODE=?");
        } catch (SQLException e) {
            throw new RuntimeException("Could not instantiate the UMLS to Source Vocabulary Converter", e);
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
                    conn.createStatement().executeUpdate("CREATE INDEX CONCEPT_INDEX ON CONCEPT_MAPPINGS (CUI, LAT, SAB, CODE, STR)");
                    System.out.print("Saving Database to Disk...");
                    conn.createStatement().execute("backup to \"" + vocabPath.replaceAll("\\\\", "/") + "UMLS/UMLS.sqlite\"");
                    System.out.println("Done");
                } catch (SQLException | IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException("Could not create the UMLS database");
                }
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
     * Creates a new {@link UMLSLookup} instance, multiple calls may be needed if objects are to be used
     * on different threads.
     *
     * @return A new {@link UMLSLookup} instance
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static UMLSLookup newLookup() {
        return new UMLSLookup();
    }

    /**
     * Retrieves equivalent codes within the various UMLS source vocabularies corresponding to a given UMLS concept <br>
     * Results from conversion can then be further
     * manipulated with the appropriate [VOCABNAME]Utils class (e.g. {@link SNOMEDCTUtils})<br>
     *
     * @param vocab   The vocabulary to retrieve
     * @param UMLSCui The UMLS cui to retrieve equivalent codes within the supplied source vocabulary for
     * @return A collection of source vocabulary codes corresponding to the UMLS cui supplied, can be empty
     * @throws SQLException if access to the lookup database fails
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public Collection<String> getSourceCodesForVocab(UMLSSourceVocabulary vocab, String UMLSCui) throws SQLException {
        Collection<String> ret = new LinkedList<>();
        getSourceCodingPS.setString(1, UMLSCui);
        getSourceCodingPS.setString(2, vocab.name());
        getSourceCodingPS.setString(3, "ENG");
        if (getSourceCodingPS.execute()) {
            ResultSet rs = getSourceCodingPS.getResultSet();
            while (rs.next()) {
                ret.add(rs.getString("CODE"));
            }
        }
        return ret;
    }

    /**
     * Performs the inverse of {@link #getSourceCodesForVocab(UMLSSourceVocabulary, String)}. retrieves a UMLS concept
     * by a source vocabulary code, if it exists. Results from this call can be chained with
     * {@link #getSourceCodesForVocab(UMLSSourceVocabulary, String)} to perform inter-vocab conversions
     *
     * @param vocab      The source vocabulary the lookup code belongs to
     * @param sourceCode The code as defined in the source vocabulary
     * @return The UMLS concept unique identifier related to the source vocabulary, or null if no such cui exists
     */
    @SuppressWarnings("unused")
    public String getUMLSCuiForSourceVocab(UMLSSourceVocabulary vocab, String sourceCode) throws SQLException {
        getCuiByCodePS.setString(1, sourceCode);
        getCuiByCodePS.setString(2, vocab.name());
        getCuiByCodePS.setString(3, "ENG");
        if (getCuiByCodePS.execute()) {
            ResultSet rs = getSourceCodingPS.getResultSet();
            if (rs.next()) { // UMLS->Source is 1:n but Source->UMLS is 1:1
                return rs.getString("CUI");
            }
        }
        return null;
    }

    /**
     * Retrieves the preferred text for a given code within the given source vocabulary
     *
     * @param vocab         The vocabulary to use
     * @param sourceConcept The concept code within that vocabulary
     * @return The preferred text within the source vocabulary, or null if concept is not found
     * @throws SQLException if access to the lookup database fails
     */
    @SuppressWarnings("unused")
    public String getSourceTermPreferredText(UMLSSourceVocabulary vocab, String sourceConcept) throws SQLException {
        getSourcePreferredPS.setString(1, vocab.name());
        getSourcePreferredPS.setString(2, sourceConcept);
        if (getSourcePreferredPS.execute()) {
            ResultSet rs = getSourcePreferredPS.getResultSet();
            if (rs.next()) {
                return rs.getString("STR");
            }
        }
        return null;
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
        CPT
    }

}
