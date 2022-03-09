package verticle

import config.Config
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.pgclient.PgConnectOptions
import io.vertx.rxjava3.core.AbstractVerticle
import io.vertx.rxjava3.ext.web.Router
import io.vertx.rxjava3.ext.web.RoutingContext
import io.vertx.rxjava3.ext.web.handler.BodyHandler
import io.vertx.rxjava3.pgclient.PgPool
import io.vertx.rxjava3.sqlclient.Tuple
import io.vertx.sqlclient.PoolOptions

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

    private lateinit var pgPoolClient: PgPool

    override fun start(promise: Promise<Void>) {
        val pgConnectOptions = PgConnectOptions()
            .setPort(5432)
            .setHost(Config.PG_HOST)
            .setDatabase(Config.PG_DATABASE)
            .setUser(Config.PG_USER)
            .setPassword(Config.PG_PASSWORD)

        val poolOptions = PoolOptions().setMaxSize(5)

        pgPoolClient = PgPool.newInstance(io.vertx.pgclient.PgPool.pool(
            vertx.delegate,
            pgConnectOptions,
            poolOptions))

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

        pgPoolClient.rxGetConnection()
            .subscribe(
                { sqlConnection ->
                    sqlConnection.query("SELECT * FROM heroes")
                        .rxExecute()
                        .subscribe(
                            { rows ->
                                val data = JsonArray()

                                for (row in rows) {
                                    data.add(JsonObject().apply {
                                        put("user_id", row.getString("user_id"))
                                        put("user_name", row.getString("user_name"))
                                        put("name_alias", row.getString("name_alias"))
                                        put("company", row.getString("company"))
                                    })
                                }

                                response = JsonObject().apply {
                                    put("success", true)
                                    put("data", data)
                                }

                                putResponse(context, 200, response)

                                sqlConnection.close()
                            },
                            {
                                response = JsonObject().apply {
                                    put("success", false)
                                    put("error", it.message)
                                }

                                putResponse(context, 500, response)

                                sqlConnection.close()
                            })
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("error", it.message)
                    }

                    putResponse(context, 500, response)
                }
            )
    }

    private fun setUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        pgPoolClient.rxGetConnection()
            .subscribe(
                { sqlConnection ->
                    sqlConnection.preparedQuery("INSERT INTO heroes(user_id, user_name, name_alias, company) " +
                            "VALUES ($1, $2, $3, $4)")
                        .rxExecute(Tuple.of(userId, userName, nameAlias, company))
                        .subscribe(
                            {
                                response = JsonObject().apply {
                                    put("success", true)
                                    put("action", "insert")
                                }

                                putResponse(context, 200, response)

                                sqlConnection.close()
                            },
                            {
                                response = JsonObject().apply {
                                    put("success", false)
                                    put("error", it.message)
                                }

                                putResponse(context, 500, response)

                                sqlConnection.close()
                            })
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("error", it.message)
                    }

                    putResponse(context, 500, response)
                }
            )
    }

    private fun updateUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        pgPoolClient.rxGetConnection()
            .subscribe(
                { sqlConnection ->
                    sqlConnection.preparedQuery("UPDATE heroes " +
                            "SET user_name=$1, name_alias=$2, company=$3 " +
                            "WHERE user_id=$4")
                        .rxExecute(Tuple.of(userName, nameAlias, company, userId))
                        .subscribe(
                            {
                                response = JsonObject().apply {
                                    put("success", true)
                                    put("action", "update")
                                }

                                putResponse(context, 200, response)

                                sqlConnection.close()
                            },
                            {
                                response = JsonObject().apply {
                                    put("success", false)
                                    put("error", it.message)
                                }

                                putResponse(context, 500, response)

                                sqlConnection.close()
                            })
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("error", it.message)
                    }

                    putResponse(context, 500, response)
                }
            )
    }

    private fun deleteUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")

        pgPoolClient.rxGetConnection()
            .subscribe(
                { sqlConnection ->
                    sqlConnection.preparedQuery("DELETE FROM heroes WHERE user_id=$1")
                        .rxExecute(Tuple.of(userId))
                        .subscribe(
                            {
                                response = JsonObject().apply {
                                    put("success", true)
                                    put("action", "delete")
                                }

                                putResponse(context, 200, response)

                                sqlConnection.close()
                            },
                            {
                                response = JsonObject().apply {
                                    put("success", false)
                                    put("error", it.message)
                                }

                                putResponse(context, 500, response)

                                sqlConnection.close()
                            })
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("error", it.message)
                    }

                    putResponse(context, 500, response)
                }
            )
    }

    private fun putResponse(context: RoutingContext, statuscode: Int, response: JsonObject) {
        context.response().statusCode = statuscode

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(response.encode())
    }
}