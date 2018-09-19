import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.persistence.neo4j.Neo4jInitializer;

module no.ssb.lds.persistence.neo4j {
    requires no.ssb.lds.persistence.api;
    requires org.json;
    requires org.neo4j.driver;
    requires java.logging;
    requires jul_to_slf4j;

    provides PersistenceInitializer with Neo4jInitializer;
}
