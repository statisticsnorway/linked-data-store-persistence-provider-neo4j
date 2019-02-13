package no.ssb.lds.core.persistence.neo4j;

import java.util.Map;

class Neo4jQueryAndParams {
    final String query;
    final Map<String, Object> params;

    Neo4jQueryAndParams(String query, Map<String, Object> params) {
        this.query = query;
        this.params = params;
    }
}
