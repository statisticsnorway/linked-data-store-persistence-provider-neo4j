package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.core.persistence.test.PersistenceIntegrationTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class Neo4jPersistenceIntegrationTest extends PersistenceIntegrationTest {

    public Neo4jPersistenceIntegrationTest() {
        super("lds-provider-neo4j-integration-test");
    }

    @BeforeClass
    public void setup() {

        persistence = new Neo4jInitializer().initialize(namespace,
                Map.of("neo4j.driver.url", "bolt://db-neo4j:7687",
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

    @Test
    public void thatReadListOfResourcesWhenOneHasBeenDeletedWorks() {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime then = now.minusSeconds(10);
        ZonedDateTime beforeThen = then.minusSeconds(10);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, toDocument(namespace, "Person", "keep", createPerson("Keep", "moa"), beforeThen), specification).blockingAwait();
            persistence.createOrOverwrite(tx, toDocument(namespace, "Person", "delete", createPerson("Delete", "mie"), beforeThen), specification).blockingAwait();
            List<JsonDocument> peopleBeforeDelete = persistence.readDocuments(tx, now, namespace, "Person", Range.unbounded()).toList().blockingGet();
            assertEquals(peopleBeforeDelete.stream().filter(jd -> !jd.deleted()).count(), 2);
            persistence.markDocumentDeleted(tx, namespace, "Person", "delete", then, null).blockingAwait();
            List<JsonDocument> peopleAfterDelete = persistence.readDocuments(tx, now, namespace, "Person", Range.unbounded()).toList().blockingGet();
            assertEquals(peopleAfterDelete.stream()
                    .filter(jd -> !jd.deleted())
                    .count(), 1);
        }
    }
}
