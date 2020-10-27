package no.ssb.lds.core.persistence.neo4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class Neo4jIndexManagement {

    private final Set<Index> wantedIndexes;
    private final boolean dropExisting;

    static class Index {
        final String label;
        final Set<String> properties;
        final boolean uniqueConstraint;

        Index(String label, List<String> properties, boolean uniqueConstraint) {
            this.label = label;
            this.properties = new LinkedHashSet<>(properties);
            this.uniqueConstraint = uniqueConstraint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Index index = (Index) o;
            return label.equals(index.label) &&
                    properties.equals(index.properties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, properties);
        }

        @Override
        public String toString() {
            return "Index{" +
                    "label='" + label + '\'' +
                    ", properties=" + properties +
                    '}';
        }
    }

    Neo4jIndexManagement(String namespace, Set<String> managedDomains, boolean dropExisting) {
        this.dropExisting = dropExisting;
        wantedIndexes = new LinkedHashSet<>();
        for (String managedDomain : managedDomains) {
            wantedIndexes.add(new Index(managedDomain + "_R", List.of("id"), true));
        }
    }

    void createIdIndices(Neo4jTransaction transaction) {
        StringBuilder constraintParam = new StringBuilder();
        StringBuilder indexParam = new StringBuilder();
        constraintParam.append("{");
        indexParam.append("{");
        int c = 0;
        int i = 0;
        for (Index index : wantedIndexes) {
            if (index.uniqueConstraint) {
                buildIndexParam(constraintParam, index, c++);
            } else {
                buildIndexParam(indexParam, index, i++);
            }
        }
        constraintParam.append("}");
        indexParam.append("}");
        transaction.executeCypher("CALL apoc.schema.assert(" + indexParam.toString() + ", " + constraintParam.toString() + ", " + dropExisting + ") YIELD label, key, keys, unique, action");
    }

    private void buildIndexParam(StringBuilder sb, Index index, int i) {
        if (i > 0) {
            sb.append(", ");
        }
        sb.append(index.label);
        sb.append(": [");
        int j = 0;
        for (String property : index.properties) {
            if (j++ > 0) {
                sb.append(", ");
            }
            sb.append("'");
            sb.append(property); // TODO quote
            sb.append("'");
        }
        sb.append("]");
    }
}
