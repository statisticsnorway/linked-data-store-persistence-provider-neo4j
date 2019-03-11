package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static no.ssb.lds.core.persistence.neo4j.SpecificationBuilder.arrayNode;
import static no.ssb.lds.core.persistence.neo4j.SpecificationBuilder.arrayRefNode;
import static no.ssb.lds.core.persistence.neo4j.SpecificationBuilder.createSpecificationAndRoot;
import static no.ssb.lds.core.persistence.neo4j.SpecificationBuilder.objectNode;
import static no.ssb.lds.core.persistence.neo4j.SpecificationBuilder.refNode;
import static no.ssb.lds.core.persistence.neo4j.SpecificationBuilder.stringNode;

/*
 * Unit tests meant to debug/verify Neo4j persistence provider without running the neo4j database.
 * i.e. These tests must not attempt to contact neo4j database.
 */
public class Neo4jPersistenceTest {

    final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void mixMapAndArray() throws IOException {
        JsonNode node = loadJson("mix-map-and-array.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "MixMapAndArray", Set.of(
                        arrayNode("child", objectNode("[]", Set.of(
                                stringNode("name"),
                                stringNode("born"),
                                arrayNode("friend", objectNode("[]", Set.of(
                                        stringNode("since"),
                                        stringNode("link")
                                )))
                        ))),
                        objectNode("name", Set.of(
                                stringNode("first"),
                                stringNode("last")
                        ))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void simpleMap() throws IOException {
        JsonNode node = loadJson("simple-map.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "SimpleMap", Set.of(
                        stringNode("one"),
                        stringNode("two"),
                        stringNode("three")
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void deeplyNestedMap() throws IOException {
        JsonNode node = loadJson("deeply-nested-map.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "DeeplyNestedMap", Set.of(
                        objectNode("the", Set.of(
                                objectNode("very", Set.of(
                                        objectNode("nested", Set.of(
                                                stringNode("one"),
                                                stringNode("two"),
                                                stringNode("three")
                                        ))
                                ))
                        ))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void simpleArray() throws IOException {
        JsonNode node = loadJson("simple-array.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "SimpleArray", Set.of(
                        arrayNode("the", stringNode("[]"))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void simpleRef() throws IOException {
        JsonNode node = loadJson("simple-ref.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "SimpleRef", Set.of(
                        refNode("foo", Set.of("Target"))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void nestedSimpleRef() throws IOException {
        JsonNode node = loadJson("nested-simple-ref.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "NestedSimpleRef", Set.of(
                        objectNode("here", Set.of(
                                refNode("foo", Set.of("Target"))
                        ))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void arrayRef() throws IOException {
        JsonNode node = loadJson("array-ref.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "ArrayRef", Set.of(
                        arrayRefNode("foo", Set.of("Target"), stringNode("[]"))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void nestedArrayRef() throws IOException {
        JsonNode node = loadJson("nested-array-ref.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "ArrayRef", Set.of(
                        objectNode("here", Set.of(
                                arrayRefNode("foo", Set.of("Target"), stringNode("[]"))
                        ))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void nullArray() throws IOException {
        JsonNode node = loadJson("null-array.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "NullArray", Set.of(
                        arrayNode("the", objectNode("[]", Set.of(
                                stringNode("one"),
                                stringNode("two")
                        ))),
                        stringNode("name")
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void emptyArray() throws IOException {
        JsonNode node = loadJson("empty-array.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "EmptyArray", Set.of(
                        arrayNode("the", objectNode("[]", Set.of(
                                stringNode("one"),
                                stringNode("two")
                        ))),
                        stringNode("name")
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void missingArray() throws IOException {
        JsonNode node = loadJson("missing-array.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "MissingArray", Set.of(
                        arrayNode("the", objectNode("[]", Set.of(
                                stringNode("one"),
                                stringNode("two")
                        ))),
                        stringNode("name")
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void emptyDocument() throws IOException {
        JsonNode node = loadJson("empty-document.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "EmptyDocument", Set.of(
                        arrayNode("the", objectNode("[]", Set.of(
                                stringNode("one"),
                                stringNode("two")
                        ))),
                        stringNode("firstname"),
                        stringNode("lastname")
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void deeplyNestedArray() throws IOException {
        JsonNode node = loadJson("deeply-nested-array.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "DeeplyNestedArray", Set.of(
                        arrayNode("the", arrayNode("[]", arrayNode("[]", stringNode("[]"))))
                ))
        ));
        createAndPresentCypher(specification, node, "1");
    }

    private JsonNode loadJson(String resourcePath) throws IOException {
        URL resource = ClassLoader.getSystemResource(resourcePath);
        return mapper.readTree(resource);
    }

    private void createAndPresentCypher(Specification specification, JsonNode node, String id) {
        String theManagedDomain = specification.getManagedDomains().iterator().next();
        Neo4jQueryAndParams qp = new Neo4jCreationalPatternFactory().creationalQueryAndParams(
                specification,
                theManagedDomain,
                List.of(new JsonDocument(new DocumentKey("neo4j-provider-test-ns", theManagedDomain, id, ZonedDateTime.now(ZoneId.of("Etc/UTC"))), node))
        );
        presentCypherWithEmbeddedParameters(qp.query, qp.params);
    }

    private void presentCypherWithEmbeddedParameters(String cypher, Map<String, Object> params) {
        String replacedCypher = resolveAndEmbeddParameters(cypher, params);
        System.out.println();
        System.out.println(replacedCypher);
        System.out.println();
    }

    private String resolveAndEmbeddParameters(String cypher, Map<String, Object> params) {
        String replacedCypher = cypher;
        replacedCypher = replacedCypher.replaceAll("\\$batch", toString(params.get("batch")));
        return replacedCypher;
    }

    private static String toString(Object dataparameter) {
        if (dataparameter == null) {
            return "null";
        }
        if (!(dataparameter instanceof Collection)) {
            if (dataparameter instanceof String) {
                return "'" + dataparameter + "'";
            }
            return String.valueOf(dataparameter);
        }
        StringBuilder sb = new StringBuilder();
        Collection<?> collection = (Collection) dataparameter;
        sb.append("[");
        Iterator<?> iterator = collection.iterator();
        if (iterator.hasNext()) {
            Object next = iterator.next();
            sb.append(toString(next));
        }
        while (iterator.hasNext()) {
            sb.append(", ");
            Object next = iterator.next();
            sb.append(toString(next));
        }
        sb.append("]");
        return sb.toString();
    }
}
