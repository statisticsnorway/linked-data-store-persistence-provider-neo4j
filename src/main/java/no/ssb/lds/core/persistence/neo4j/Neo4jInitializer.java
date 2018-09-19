package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.persistence.ProviderName;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

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
                "neo4j.driver.password"
        );
    }

    @Override
    public Persistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains) {
        JavaUtilLoggingInitializer.initialize();
        String neo4jDriverURL = configuration.get("neo4j.driver.url");
        String neo4jDriverUsername = configuration.get("neo4j.driver.username");
        String neo4jDriverPassword = configuration.get("neo4j.driver.password");
        boolean logCypher = Boolean.parseBoolean(configuration.get("neo4j.cypher.show"));
        Driver driver = open(neo4jDriverURL, neo4jDriverUsername, neo4jDriverPassword);
        return new Neo4JCORDPersistence(new Neo4jPersistenceProvider(driver, defaultNamespace, managedDomains, logCypher), logCypher);
    }

    private static Driver open(String neo4jDriverURL, String neo4jDriverUsername, String neo4jDriverPassword) {
        return GraphDatabase.driver(neo4jDriverURL, AuthTokens.basic(neo4jDriverUsername, neo4jDriverPassword));
    }
}
