package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.core.persistence.test.PersistenceIntegrationTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class Neo4jPersistenceIntegrationTest extends PersistenceIntegrationTest {

    public Neo4jPersistenceIntegrationTest() {
        super("lds-provider-neo4j-integration-test");
    }

    @BeforeClass
    public void setup() {

        persistence = new Neo4jInitializer().initialize(namespace,
                Map.of("neo4j.driver.url", "bolt://localhost:7687",
                        "neo4j.driver.username", "neo4j",
                        "neo4j.driver.password", "PasSW0rd",
                        "neo4j.cypher.show", "true"),
                Set.of("Person", "Address", "FunkyLongAddress")
        );

    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
