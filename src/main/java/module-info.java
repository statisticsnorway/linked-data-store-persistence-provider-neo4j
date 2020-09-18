import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.persistence.neo4j.Neo4jInitializer;

module no.ssb.lds.persistence.neo4j {
    requires no.ssb.lds.persistence.api;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires org.neo4j.driver;
    requires java.logging;
    requires jul_to_slf4j;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires graphql.java;
    requires org.antlr.antlr4.runtime;

    provides PersistenceInitializer with Neo4jInitializer;

    exports no.ssb.lds.cypher;
}
