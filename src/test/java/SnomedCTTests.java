import edu.mayo.bsi.nlp.vts.SNOMEDCT;
import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit tests verifying utility methods in {@link SNOMEDCT}
 */
public class SnomedCTTests {
    /**
     * Test for {@link SNOMEDCT#isChild(String, String)}
     */
    @Test
    public void testHierarchy() {
        System.setProperty("vocab.src.dir", System.getProperty("user.dir"));
        // Test true case (direct parent)
        Assert.assertTrue(SNOMEDCT.isChild("419303009", "419492006"));
        // Test reverse of true case (parent->child)
        Assert.assertFalse(SNOMEDCT.isChild("419492006", "419303009"));
        // Test true case (indirect parent)
        Assert.assertTrue(SNOMEDCT.isChild("25064002", "404684003"));
        // Test reverse of indirect true case (parent->child)
        Assert.assertFalse(SNOMEDCT.isChild("404684003", "25064002"));
    }
}
