package io.quarkus.redis.it.lettuce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;

@QuarkusTest
class LettuceBackendTest {

    String getKey(String k) {
        return k;
    }

    @Test
    public void ping() {
        RestAssured.given()
                .when()
                .get("/lettuce/ping")
                .then()
                .statusCode(200)
                .body(CoreMatchers.is("PONG"));
    }

    @Test
    public void pingMutiny() {
        RestAssured.given()
                .when()
                .get("/lettuce/ping/mutiny")
                .then()
                .statusCode(200)
                .body(CoreMatchers.is("PONG"));
    }

    @Test
    public void valueSetGet() {
        String key = getKey("value-sync");
        String value = "lettuce-value";

        RestAssured.given()
                .when()
                .get("/lettuce/value/" + key)
                .then()
                .statusCode(204);

        RestAssured.given()
                .body(value)
                .when()
                .post("/lettuce/value/" + key)
                .then()
                .statusCode(204);

        RestAssured.given()
                .when()
                .get("/lettuce/value/" + key)
                .then()
                .statusCode(200)
                .body(CoreMatchers.is(value));
    }

    @Test
    public void valueGetReactive() {
        String key = getKey("value-reactive");
        String value = "lettuce-reactive";

        RestAssured.given()
                .body(value)
                .when()
                .post("/lettuce/value/" + key)
                .then()
                .statusCode(204);

        RestAssured.given()
                .when()
                .get("/lettuce/reactive/" + key)
                .then()
                .statusCode(200)
                .body(CoreMatchers.is(value));
    }

    @Test
    public void selectChangesDatabase() {
        String key = getKey("select-key");
        try {
            RestAssured.given().body("db0").when().post("/lettuce/value/" + key).then().statusCode(204);

            RestAssured.given().when().post("/lettuce/select/1").then().statusCode(204);

            RestAssured.given().when().get("/lettuce/value/" + key).then().statusCode(204);

            RestAssured.given().body("db1").when().post("/lettuce/value/" + key).then().statusCode(204);
            RestAssured.given().when().get("/lettuce/value/" + key).then()
                    .statusCode(200).body(CoreMatchers.is("db1"));

            RestAssured.given().when().post("/lettuce/select/0").then().statusCode(204);
            RestAssured.given().when().get("/lettuce/value/" + key).then()
                    .statusCode(200).body(CoreMatchers.is("db0"));
        } finally {
            RestAssured.given().when().post("/lettuce/select/1").then().statusCode(204);
            RestAssured.given().when().delete("/lettuce/flushall").then().statusCode(204);
            RestAssured.given().when().post("/lettuce/select/0").then().statusCode(204);
        }
    }

    @Test
    public void flushall() {
        String key = getKey("flush-key");

        RestAssured.given()
                .body("to-be-flushed")
                .when()
                .post("/lettuce/value/" + key)
                .then()
                .statusCode(204);

        RestAssured.given()
                .when()
                .delete("/lettuce/flushall")
                .then()
                .statusCode(204);

        RestAssured.given()
                .when()
                .get("/lettuce/value/" + key)
                .then()
                .statusCode(204);
    }

    @Test
    public void keyExistsAndDel() {
        String key = getKey("key-exists");
        RestAssured.given().body("v").when().post("/lettuce/value/" + key).then().statusCode(204);

        RestAssured.given().when().get("/lettuce/key/exists/" + key).then()
                .statusCode(200).body(CoreMatchers.is("true"));

        RestAssured.given().when().delete("/lettuce/key/" + key).then()
                .statusCode(200).body(CoreMatchers.is("1"));

        RestAssured.given().when().get("/lettuce/key/exists/" + key).then()
                .statusCode(200).body(CoreMatchers.is("false"));
    }

    @Test
    public void keyExpireTtlPersist() {
        String key = getKey("key-ttl");
        RestAssured.given().body("v").when().post("/lettuce/value/" + key).then().statusCode(204);

        RestAssured.given().when().post("/lettuce/key/expire/" + key + "/100").then()
                .statusCode(200).body(CoreMatchers.is("true"));

        long ttl = Long.parseLong(RestAssured.given().when().get("/lettuce/key/ttl/" + key)
                .then().statusCode(200).extract().asString());
        assert ttl > 0 && ttl <= 100 : "expected ttl in (0, 100], got " + ttl;

        RestAssured.given().when().post("/lettuce/key/persist/" + key).then()
                .statusCode(200).body(CoreMatchers.is("true"));

        RestAssured.given().when().get("/lettuce/key/ttl/" + key).then()
                .statusCode(200).body(CoreMatchers.is("-1"));
    }

    @Test
    public void keyReactiveTtl() {
        String key = getKey("key-reactive-ttl");
        RestAssured.given().body("v").when().post("/lettuce/value/" + key).then().statusCode(204);
        RestAssured.given().when().post("/lettuce/key/expire/" + key + "/200").then().statusCode(200);

        long ttl = Long.parseLong(RestAssured.given().when().get("/lettuce/key/reactive/ttl/" + key)
                .then().statusCode(200).extract().asString());
        assert ttl > 0 && ttl <= 200 : "expected ttl in (0, 200], got " + ttl;
    }

    @Test
    public void keyRenameAndCopy() {
        String src = getKey("key-src");
        String dst = getKey("key-dst");
        String copyDst = getKey("key-copy-dst");
        RestAssured.given().body("payload").when().post("/lettuce/value/" + src).then().statusCode(204);

        RestAssured.given().when().post("/lettuce/key/copy/" + src + "/" + copyDst).then()
                .statusCode(200).body(CoreMatchers.is("true"));
        RestAssured.given().when().get("/lettuce/value/" + copyDst).then()
                .statusCode(200).body(CoreMatchers.is("payload"));

        RestAssured.given().when().post("/lettuce/key/rename/" + src + "/" + dst).then()
                .statusCode(204);
        RestAssured.given().when().get("/lettuce/key/exists/" + src).then()
                .statusCode(200).body(CoreMatchers.is("false"));
        RestAssured.given().when().get("/lettuce/value/" + dst).then()
                .statusCode(200).body(CoreMatchers.is("payload"));
    }

    @Test
    public void keyType() {
        String key = getKey("key-type");
        RestAssured.given().body("v").when().post("/lettuce/value/" + key).then().statusCode(204);
        RestAssured.given().when().get("/lettuce/key/type/" + key).then()
                .statusCode(200).body(CoreMatchers.is("STRING"));

        String missing = getKey("key-type-missing");
        RestAssured.given().when().get("/lettuce/key/type/" + missing).then()
                .statusCode(200).body(CoreMatchers.is("NONE"));
    }

    @Test
    public void keyScan() {
        String prefix = getKey("scan-");
        for (int i = 0; i < 5; i++) {
            RestAssured.given().body("v").when().post("/lettuce/value/" + prefix + i).then().statusCode(204);
        }
        RestAssured.given().queryParam("match", prefix + "*").when().get("/lettuce/key/scan").then()
                .statusCode(200)
                .body("$", CoreMatchers.hasItems(prefix + "0", prefix + "1", prefix + "2", prefix + "3", prefix + "4"));
    }

    @Test
    public void withConnectionBlockingClientIds() {
        String body = RestAssured.given().when().get("/lettuce/with-connection/client-ids")
                .then().statusCode(200).extract().asString();
        String[] parts = body.split(",");
        long inside1 = Long.parseLong(parts[0]);
        long inside2 = Long.parseLong(parts[1]);
        long outside = Long.parseLong(parts[2]);
        assertTrue(inside1 > 0 && inside2 > 0 && outside > 0, () -> "expected positive ids, got " + body);
        assertEquals(inside1, inside2, () -> "expected stable id inside block, got " + body);
        assertNotEquals(inside1, outside, () -> "expected fresh connection inside block, got " + body);
    }

    @Test
    public void withConnectionReactiveClientIds() {
        String body = RestAssured.given().when().get("/lettuce/with-connection/reactive/client-ids")
                .then().statusCode(200).extract().asString();
        String[] parts = body.split(",");
        long inside1 = Long.parseLong(parts[0]);
        long inside2 = Long.parseLong(parts[1]);
        long outside = Long.parseLong(parts[2]);
        assertTrue(inside1 > 0 && inside2 > 0 && outside > 0, () -> "expected positive ids, got " + body);
        assertEquals(inside1, inside2, () -> "expected stable id inside block, got " + body);
        assertNotEquals(inside1, outside, () -> "expected fresh connection inside block, got " + body);
    }

    @Test
    public void withConnectionNestedReusesConnection() {
        String body = RestAssured.given().when().get("/lettuce/with-connection/nested")
                .then().statusCode(200).extract().asString();
        String[] parts = body.split(",");
        long outer = Long.parseLong(parts[0]);
        long inner = Long.parseLong(parts[1]);
        assertEquals(outer, inner, () -> "expected nested withConnection to reuse outer connection, got " + body);
    }

    @Test
    public void withTransactionBlocking() {
        String key = getKey("tx-blocking");
        String body = RestAssured.given().body("v1").when().post("/lettuce/with-transaction/blocking/" + key)
                .then().statusCode(200).extract().asString();
        assertEquals("false,2,v1", body);
        RestAssured.given().when().get("/lettuce/value/" + key).then()
                .statusCode(200).body(CoreMatchers.is("v1"));
    }

    @Test
    public void withTransactionReactive() {
        String key = getKey("tx-reactive");
        String body = RestAssured.given().body("rv1").when().post("/lettuce/with-transaction/reactive/" + key)
                .then().statusCode(200).extract().asString();
        assertEquals("false,2,rv1", body);
        RestAssured.given().when().get("/lettuce/value/" + key).then()
                .statusCode(200).body(CoreMatchers.is("rv1"));
    }

    @Test
    public void withTransactionDiscard() {
        String key = getKey("tx-discard");
        String body = RestAssured.given().body("v").when().post("/lettuce/with-transaction/discard/" + key)
                .then().statusCode(200).extract().asString();
        assertEquals("true,null", body);
        RestAssured.given().when().get("/lettuce/value/" + key).then().statusCode(204);
    }

    @Test
    public void withTransactionOptimistic() {
        String key = getKey("tx-optimistic");
        RestAssured.given().body("10").when().post("/lettuce/value/" + key).then().statusCode(204);
        String body = RestAssured.given().body("0").when().post("/lettuce/with-transaction/optimistic/" + key)
                .then().statusCode(200).extract().asString();
        assertEquals("false,10,100", body);
    }

    @Test
    public void withTransactionKey() {
        String key = getKey("tx-key");
        String body = RestAssured.given().when().post("/lettuce/with-transaction/key/" + key)
                .then().statusCode(200).extract().asString();
        assertEquals("false,4,true,true,true,STRING", body);
    }
}
