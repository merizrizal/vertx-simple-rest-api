package verticle

import config.Config
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Single
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.redis.client.RedisOptions
import io.vertx.rxjava3.core.AbstractVerticle
import io.vertx.rxjava3.ext.web.Router
import io.vertx.rxjava3.ext.web.RoutingContext
import io.vertx.rxjava3.ext.web.handler.BodyHandler
import io.vertx.rxjava3.redis.client.Redis
import io.vertx.rxjava3.redis.client.RedisAPI

class HttpServerVerticle : AbstractVerticle() {
    private lateinit var redis: Redis

    override fun start(promise: Promise<Void>) {
        val redisOptions = RedisOptions()
            .setConnectionString("redis://:${Config.REDIS_PASSWORD}@${Config.REDIS_HOST}/${Config.REDIS_DATABASE}")

        redis = Redis(io.vertx.redis.client.Redis.createClient(vertx.delegate, redisOptions))

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

        redis
            .rxConnect()
            .subscribe(
                { redisConnection ->
                    RedisAPI.api(redisConnection)
                        .rxHmset(ArrayList<String>().apply {
                            add("heroes:${userId}")
                            add("userName")
                            add(userName)
                            add("nameAlias")
                            add(nameAlias)
                            add("company")
                            add(company)
                        })
                        .subscribe(
                            {
                                response = JsonObject().apply {
                                    put("success", true)
                                    put("action", "insert")
                                }

                                putResponse(context, 200, response)

                                redisConnection.rxClose()
                            },
                            {
                                response = JsonObject().apply {
                                    put("success", false)
                                    put("message", it.message)
                                }

                                putResponse(context, 500, response)

                                redisConnection.rxClose()

                            })
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("message", it.message)
                    }

                    putResponse(context, 500, response)
                })
    }

    private fun updateUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        redis
            .rxConnect()
            .subscribe(
                { redisConnection ->
                    val redisApi = RedisAPI.api(redisConnection)
                    redisApi
                        .rxHmget(ArrayList<String>().apply {
                            add("heroes:${userId}")
                            add("userName")
                        })
                        .toObservable()
                        .map {
                            null != it.iterator().next()
                        }
                        .flatMapMaybe { exist ->
                            if (exist) {
                                redisApi
                                    .rxHmset(ArrayList<String>().apply {
                                        add("heroes:${userId}")

                                        if (null != userName) {
                                            add("userName")
                                            add(userName)
                                        }

                                        if (null != nameAlias) {
                                            add("nameAlias")
                                            add(nameAlias)
                                        }

                                        if (null != company) {
                                            add("company")
                                            add(company)
                                        }
                                    })
                            } else {
                                Maybe.just(false)
                            }
                        }
                        .subscribe(
                            {
                                response = if (it is Boolean && !it) {
                                    JsonObject().apply {
                                        put("success", false)
                                        put("message", "Data doesn't exist")
                                    }
                                } else {
                                    JsonObject().apply {
                                        put("success", true)
                                        put("action", "update")
                                    }
                                }

                                putResponse(context, 200, response)

                                redisConnection.rxClose()
                            },
                            {
                                response = JsonObject().apply {
                                    put("success", false)
                                    put("message", it.message)
                                }

                                putResponse(context, 500, response)

                                redisConnection.rxClose()
                            })
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("message", it.message)
                    }

                    putResponse(context, 500, response)
                })
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