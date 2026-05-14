package io.quarkus.redis.it.lettuce;

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
}
