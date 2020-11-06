package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
                        stringNode("id"),
                        arrayNode("child", objectNode("[]", Set.of(
                                stringNode("name"),
                                stringNode("born"),
                                arrayNode("friend", objectNode("[]", Set.of(
                                        stringNode("since"),
                                        refNode("link", Set.of("Person"))
                                )))
                        ))),
                        objectNode("name", Set.of(
                                stringNode("first"),
                                stringNode("last")
                        ))
                ))
        ), "interface Someone {\n" +
                "}\n" +
                "type MixMapAndArray implements Someone {\n" +
                "  id: ID!\n" +
                "  name: Name @relation(name: \"name\")\n" +
                "  child: [Child] @relation(name: \"child\")\n" +
                "}\n" +
                "type Child {\n" +
                "  name: String\n" +
                "  born: String\n" +
                "  friend: [Friend] @relation(name: \"friend\")\n" +
                "}\n" +
                "type Friend {\n" +
                "  since: String\n" +
                "  link: Person @link @relation(name: \"link\")\n" +
                "}\n" +
                "type Person implements Someone {\n" +
                "  age: Int\n" +
                "  name: Name @relation(name: \"name\")\n" +
                "}\n" +
                "type Name {\n" +
                "  first: String\n" +
                "  last: String\n" +
                "}");
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void createFriend() throws IOException {
        JsonNode node = loadJson("mix-map-and-array-friend.json");
        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "MixMapAndArray", Set.of(
                        stringNode("id"),
                        arrayNode("child", objectNode("[]", Set.of(
                                stringNode("name"),
                                stringNode("born"),
                                arrayNode("friend", objectNode("[]", Set.of(
                                        stringNode("since"),
                                        refNode("link", Set.of("Person"))
                                )))
                        ))),
                        objectNode("name", Set.of(
                                stringNode("first"),
                                stringNode("last")
                        ))
                )),
                objectNode(SpecificationElementType.MANAGED, "Person", Set.of(
                        objectNode("name", Set.of(
                                stringNode("first"),
                                stringNode("last")
                        ))
                ))
        ), "interface Someone {\n" +
                "}\n" +
                "type MixMapAndArray implements Someone {\n" +
                "  id: ID!\n" +
                "  name: Name @relation(name: \"name\")\n" +
                "  child: [Child] @relation(name: \"child\")\n" +
                "}\n" +
                "type Child {\n" +
                "  name: String\n" +
                "  born: String\n" +
                "  friend: [Friend] @relation(name: \"friend\")\n" +
                "}\n" +
                "type Friend {\n" +
                "  since: String\n" +
                "  link: Person @link @relation(name: \"link\")\n" +
                "}\n" +
                "type Person implements Someone {\n" +
                "  age: Int\n" +
                "  name: Name @relation(name: \"name\")\n" +
                "}\n" +
                "type Name {\n" +
                "  first: String\n" +
                "  last: String\n" +
                "}");
        createAndPresentCypher(specification, "Person", node, "nate");
    }

    @Test
    public void simpleMap() throws IOException {
        JsonNode node = loadJson("simple-map.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "SimpleMap", Set.of(
                                stringNode("one"),
                                stringNode("two"),
                                stringNode("three")
                        ))
                ),
                "type SimpleMap @domain {\n" +
                        "  one: String\n" +
                        "  two: String\n" +
                        "  three: String\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void deeplyNestedMap() throws IOException {
        JsonNode node = loadJson("deeply-nested-map.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
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
                ),
                "type DeeplyNestedMap @domain {\n" +
                        "  the: The\n" +
                        "}\n" +
                        "type The {\n" +
                        "  very: Very\n" +
                        "}\n" +
                        "type Very {\n" +
                        "  nested: Nested\n" +
                        "}\n" +
                        "type Nested {\n" +
                        "  one: String\n" +
                        "  two: String\n" +
                        "  three: String\n" +
                        "}\n"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void simpleArray() throws IOException {
        JsonNode node = loadJson("simple-array.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "SimpleArray", Set.of(
                                arrayNode("the", stringNode("[]"))
                        ))
                ),
                "type SimpleArray @domain {\n" +
                        "  the: [String]\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void simpleRef() throws IOException {
        JsonNode node = loadJson("simple-ref.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "SimpleRef", Set.of(
                                refNode("foo", Set.of("Target"))
                        ))
                ),
                "type SimpleRef @domain {\n" +
                        "  foo: Target @link\n" +
                        "}\n" +
                        "type Target @domain {\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void nestedSimpleRef() throws IOException {
        JsonNode node = loadJson("nested-simple-ref.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "NestedSimpleRef", Set.of(
                                objectNode("here", Set.of(
                                        refNode("foo", Set.of("Target"))
                                ))
                        ))
                ),
                "type NestedSimpleRef @domain {\n" +
                        "  here: Here\n" +
                        "}\n" +
                        "type Here {\n" +
                        "  foo: Target @link\n" +
                        "}\n" +
                        "type Target @domain {\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void arrayRef() throws IOException {
        JsonNode node = loadJson("array-ref.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "ArrayRef", Set.of(
                                arrayRefNode("foo", Set.of("Target"), stringNode("[]"))
                        ))
                ),
                "type ArrayRef @domain {\n" +
                        "  foo: [Target] @link\n" +
                        "}\n" +
                        "type Target @domain {\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void nestedArrayRef() throws IOException {
        JsonNode node = loadJson("nested-array-ref.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "ArrayRef", Set.of(
                                objectNode("here", Set.of(
                                        arrayRefNode("foo", Set.of("Target"), stringNode("[]"))
                                ))
                        ))
                ),
                "type ArrayRef @domain {\n" +
                        "  here: Here\n" +
                        "}\n" +
                        "type Here {\n" +
                        "  foo: [Target] @link\n" +
                        "}\n" +
                        "type Target @domain {\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void nullArray() throws IOException {
        JsonNode node = loadJson("null-array.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "NullArray", Set.of(
                                arrayNode("the", objectNode("[]", Set.of(
                                        stringNode("one"),
                                        stringNode("two")
                                ))),
                                stringNode("name")
                        ))
                ),
                "type NullArray @domain {\n" +
                        "  the: [The]\n" +
                        "  name: String\n" +
                        "}\n" +
                        "type The {\n" +
                        "  one: String\n" +
                        "  two: String\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void emptyArray() throws IOException {
        JsonNode node = loadJson("empty-array.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "EmptyArray", Set.of(
                                arrayNode("the", objectNode("[]", Set.of(
                                        stringNode("one"),
                                        stringNode("two")
                                ))),
                                stringNode("name")
                        ))
                ),
                "type EmptyArray @domain {\n" +
                        "  the: [The]\n" +
                        "  name: String\n" +
                        "}\n" +
                        "type The {\n" +
                        "  one: String\n" +
                        "  two: String\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void missingArray() throws IOException {
        JsonNode node = loadJson("missing-array.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "MissingArray", Set.of(
                                arrayNode("the", objectNode("[]", Set.of(
                                        stringNode("one"),
                                        stringNode("two")
                                ))),
                                stringNode("name")
                        ))
                ),
                "type MissingArray @domain {\n" +
                        "  the: [The]\n" +
                        "  name: String\n" +
                        "}\n" +
                        "type The {\n" +
                        "  one: String\n" +
                        "  two: String\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    public void emptyDocument() throws IOException {
        JsonNode node = loadJson("empty-document.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "EmptyDocument", Set.of(
                                arrayNode("the", objectNode("[]", Set.of(
                                        stringNode("one"),
                                        stringNode("two")
                                ))),
                                stringNode("firstname"),
                                stringNode("lastname")
                        ))
                ),
                "type EmptyDocument @domain {\n" +
                        "  the: [The]\n" +
                        "  firstname: String\n" +
                        "  lastname: String\n" +
                        "}\n" +
                        "type The {\n" +
                        "  one: String\n" +
                        "  two: String\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    @Test
    @Ignore // nested arrays are unsupported in LDS because it is unsupported by GraphQL.
    public void deeplyNestedArray() throws IOException {
        JsonNode node = loadJson("deeply-nested-array.json");
        Specification specification = createSpecificationAndRoot(
                Set.of(
                        objectNode(SpecificationElementType.MANAGED, "DeeplyNestedArray", Set.of(
                                arrayNode("the", arrayNode("[]", arrayNode("[]", stringNode("[]"))))
                        ))
                ),
                "type DeeplyNestedArray @domain {\n" +
                        "  the: [[[String]]]\n" +
                        "}"
        );
        createAndPresentCypher(specification, node, "1");
    }

    private JsonNode loadJson(String resourcePath) throws IOException {
        URL resource = ClassLoader.getSystemResource(resourcePath);
        return mapper.readTree(resource);
    }

    private void createAndPresentCypher(Specification specification, JsonNode node, String id) {
        String theManagedDomain = specification.getManagedDomains().iterator().next();
        createAndPresentCypher(specification, theManagedDomain, node, id);
    }

    private void createAndPresentCypher(Specification specification, String theManagedDomain, JsonNode node, String id) {
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
            if (dataparameter instanceof ZonedDateTime) {
                return "datetime('" + dataparameter + "')";
            }
            if (dataparameter instanceof LocalDateTime) {
                return "localdatetime('" + dataparameter + "')";
            }
            if (dataparameter instanceof LocalDate) {
                return "date('" + dataparameter + "')";
            }
            if (dataparameter instanceof LocalTime) {
                return "localtime('" + dataparameter + "')";
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
