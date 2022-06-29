package verticle

import config.Config
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.redis.client.RedisOptions
import io.vertx.rxjava3.core.AbstractVerticle
import io.vertx.rxjava3.ext.web.Router
import io.vertx.rxjava3.ext.web.RoutingContext
import io.vertx.rxjava3.ext.web.handler.BodyHandler
import io.vertx.rxjava3.pgclient.PgPool
import io.vertx.rxjava3.redis.client.Redis
import io.vertx.rxjava3.redis.client.RedisAPI

class HttpServerVerticle : AbstractVerticle() {
    private lateinit var redisApi: RedisAPI

    override fun start(promise: Promise<Void>) {
        val redisOptions = RedisOptions()
            .setConnectionString("redis://:${Config.REDIS_PASSWORD}@${Config.REDIS_HOST}/${Config.REDIS_DATABASE}")

        Redis(io.vertx.redis.client.Redis.createClient(vertx.delegate, redisOptions))
            .rxConnect()
            .subscribe(
                { redisConnection ->
                    redisConnection.handler {
                        println(it)
                        if (it.size() > 0) {
                            if (it[0].toString() == "message") {
                                println("Received message at channel ${it[1]}: ${it[2]}")
                            }
                        }
                    }

                    redisApi = RedisAPI.api(redisConnection)
                },
                { failure -> promise.fail(failure.cause) })

        val router = Router.router(vertx).apply {
            get("/api/users").handler(this@HttpServerVerticle::getUsers)
            post("/api/users").handler(BodyHandler.create()).handler(this@HttpServerVerticle::setUser)
            put("/api/users").handler(BodyHandler.create()).handler(this@HttpServerVerticle::updateUser)
            delete("/api/users").handler(this@HttpServerVerticle::deleteUser)
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
        var response: JsonObject

//        for (row in rows) {
//            data.add(JsonObject().apply {
//                put("user_id", row.getString("user_id"))
//                put("user_name", row.getString("user_name"))
//                put("name_alias", row.getString("name_alias"))
//                put("company", row.getString("company"))
//            })
//        }
//
        response = JsonObject().apply {
            put("success", true)
            put("data", true)
        }

        putResponse(context, 200, response)
    }

    private fun setUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        response = JsonObject().apply {
            put("success", false)
            put("error", true)
        }

        putResponse(context, 500, response)
    }

    private fun updateUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")
    }

    private fun deleteUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
    }

    private fun putResponse(context: RoutingContext, statuscode: Int, response: JsonObject) {
        context.response().statusCode = statuscode

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(response.encode())
    }
}