package verticle

import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.rxjava3.core.AbstractVerticle
import io.vertx.rxjava3.ext.web.Router
import io.vertx.rxjava3.ext.web.RoutingContext
import io.vertx.rxjava3.ext.web.handler.BodyHandler

class HttpServerVerticle : AbstractVerticle() {
    private val users = JsonObject().put(
            "users",
            JsonObject().put(
                "tonys",
                JsonObject().apply {
                    put("user_id", "tonys")
                    put("user_name", "Tony Stark")
                    put("name_alias", "Iron Man")
                    put("company", "Stark Industries")
                }))

    override fun start(promise: Promise<Void>) {
        val router = Router.router(vertx).apply {
            get("/api/users").handler(this@HttpServerVerticle::getUsers)
            post("/api/users").handler(BodyHandler.create()).handler(this@HttpServerVerticle::setUser)
            put("/api/users").handler(BodyHandler.create()).handler(this@HttpServerVerticle::updateUser)
            delete("/api/users").handler(BodyHandler.create()).handler(this@HttpServerVerticle::deleteUser)
        }

        vertx
            .createHttpServer()
            .requestHandler(router)
            .rxListen(8282)
            .subscribe(
                { promise.complete() },
                { failure -> promise.fail(failure.cause) })
    }

    private fun getUsers(context: RoutingContext) {
        context.response().statusCode = 200

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(users.encode())
    }

    private fun setUser(context: RoutingContext) {
        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        users.getJsonObject("users").put(
                userId,
                JsonObject().apply {
                    put("user_id", userId)
                    put("user_name", userName)
                    put("name_alias", nameAlias)
                    put("company", company)
                })

        val response = JsonObject().apply {
            put("success", true)
            put("action", "insert")
            put("current_rows", users)
        }

        context.response().statusCode = 200

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(response.encode())
    }

    private fun updateUser(context: RoutingContext) {
        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        users.apply {
            getJsonObject("users").getJsonObject(userId).apply {
                put("user_name", userName)
                put("name_alias", nameAlias)
                put("company", company)
            }
        }

        val response = JsonObject().apply {
            put("success", true)
            put("action", "insert")
            put("current_rows", users)
        }

        context.response().statusCode = 200

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(response.encode())
    }

    private fun deleteUser(context: RoutingContext) {
        val userId = context.request().getParam("user_id")

        users.getJsonObject("users").remove(userId)

        val response = JsonObject().apply {
            put("success", true)
            put("action", "insert")
            put("current_rows", users)
        }

        context.response().statusCode = 200

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(response.encode())
    }
}