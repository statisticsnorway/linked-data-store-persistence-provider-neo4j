package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.PersistenceException;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class Neo4jPersistenceProvider {

    private final Driver driver;
    private final String namespaceDefault;
    private final boolean logCypher;

    static class Index {
        final String label;
        final Set<String> properties;

        Index(String label, List<String> properties) {
            this.label = label;
            this.properties = new LinkedHashSet<>(properties);
        }

        String createUniqueConstraintCypher() {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE CONSTRAINT ON (n:");
            sb.append(label);
            sb.append(") ASSERT ");
            sb.append(properties.stream().map(k -> "n." + k).collect(Collectors.joining(", ")));
            sb.append(" IS UNIQUE;");
            return sb.toString();
        }

        String createIndexCypher() {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE INDEX ON :");
            sb.append(label);
            sb.append("(");
            sb.append(properties.stream().collect(Collectors.joining(", ")));
            sb.append(");");
            return sb.toString();
        }

        String deleteCypherStatement() {
            StringBuilder sb = new StringBuilder();
            sb.append("DROP INDEX ON :");
            sb.append(label);
            sb.append("(");
            sb.append(properties.stream().collect(Collectors.joining(", ")));
            sb.append(");");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Index index = (Index) o;
            return Objects.equals(label, index.label) &&
                    Objects.equals(properties, index.properties);
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

    <T> T writeTransaction(Function<Transaction, T> work) {
        try (Session session = driver.session(AccessMode.WRITE)) {
            return session.writeTransaction(tx -> work.apply(tx));
        } catch (RuntimeException e) {
            throw new PersistenceException(e);
        }
    }

    <T> T readTransaction(Function<Transaction, T> work) {
        try (Session session = driver.session(AccessMode.READ)) {
            return session.readTransaction(tx -> work.apply(tx));
        } catch (RuntimeException e) {
            throw new PersistenceException(e);
        }
    }

    Neo4jPersistenceProvider(Driver driver, String namespaceDefault, Set<String> managedDomains, boolean logCypher) {
        this.driver = driver;
        this.namespaceDefault = namespaceDefault;
        this.logCypher = logCypher;
        Set<Index> currentIndexes = new LinkedHashSet<>();
        readTransaction(tx -> {
            StatementResult statementResult = tx.run(String.format("CALL db.indexes"));
            while (statementResult.hasNext()) {
                Record record = statementResult.next();
                Value labelValue = record.get("label");
                Value properties = record.get("properties");
                String label = labelValue.asString();
                List<String> propertyList = properties.asList(Value::asString);
                currentIndexes.add(new Index(label, propertyList));
            }
            return null;
        });
        Set<Index> wantedIndexes = new LinkedHashSet<>();
        for (String managedDomain : managedDomains) {
            wantedIndexes.add(new Index(nodeLabel(namespaceDefault, managedDomain, "M"), List.of("id")));
        }
        for (Index index : currentIndexes) {
            if (!wantedIndexes.contains(index)) {
                System.out.format("Not using index: %s. Use the following cypher statement to drop: %s%n", index.toString(), index.deleteCypherStatement());
            }
        }
        for (Index index : wantedIndexes) {
            if (!currentIndexes.contains(index)) {
                System.out.format("Creating index using cypher: %s%n", index.createUniqueConstraintCypher());
                writeTransaction(tx -> {
                    tx.run(index.createUniqueConstraintCypher());
                    return null;
                });
            }
        }
    }

    String nodeLabel(String namespace, String entity, String classifier) {
        return (namespaceDefault.equals(namespace) ? "" : namespace + "__") + entity + ("M".equals(classifier) ? "" : "__" + classifier);
    }

    Driver driver() {
        return driver;
    }

    Neo4jPersistenceReader newReader(Session session, Transaction tx) {
        return new Neo4jPersistenceReader(session, tx, namespaceDefault, logCypher);
    }

    Neo4jPersistenceWriter newWriter(Transaction tx) {
        return new Neo4jPersistenceWriter(tx, namespaceDefault, logCypher);
    }


}
