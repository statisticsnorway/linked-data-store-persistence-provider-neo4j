package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.persistence.ProviderName;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

@ProviderName("neo4j")
public class Neo4jInitializer implements PersistenceInitializer {

    static class JavaUtilLoggingInitializer {
        static {
            JavaUtilLoggerBridge.installJavaUtilLoggerBridgeHandler(Level.INFO);
        }

        static void initialize() {
        }
    }

    @Override
    public String persistenceProviderId() {
        return "neo4j";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "neo4j.cypher.show",
                "neo4j.driver.url",
                "neo4j.driver.username",
                "neo4j.driver.password",
                "neo4j.schema.drop-existing-indexes"
        );
    }

    @Override
    public RxJsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains, Specification specification) {
        JavaUtilLoggingInitializer.initialize();
        String neo4jDriverURL = configuration.get("neo4j.driver.url");
        String neo4jDriverUsername = configuration.get("neo4j.driver.username");
        String neo4jDriverPassword = configuration.get("neo4j.driver.password");
        boolean logCypher = Boolean.parseBoolean(configuration.get("neo4j.cypher.show"));
        boolean dropExistingIndexes = Boolean.parseBoolean(configuration.get("neo4j.schema.drop-existing-indexes"));
        Driver driver = open(neo4jDriverURL, neo4jDriverUsername, neo4jDriverPassword);
        Neo4jTransactionFactory transactionFactory = new Neo4jTransactionFactory(driver, logCypher);
        Neo4jIndexManagement indexManagement = new Neo4jIndexManagement(defaultNamespace, managedDomains, dropExistingIndexes);
        try (Neo4jTransaction tx = transactionFactory.createTransaction(false)) {
            indexManagement.createIdIndices(tx);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return new Neo4jPersistence(transactionFactory);
    }

    private static Driver open(String neo4jDriverURL, String neo4jDriverUsername, String neo4jDriverPassword) {
        return GraphDatabase.driver(neo4jDriverURL, AuthTokens.basic(neo4jDriverUsername, neo4jDriverPassword));
    }
}
