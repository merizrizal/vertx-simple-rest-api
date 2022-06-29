package verticle

import config.Config
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.redis.client.RedisOptions
import io.vertx.rxjava3.core.AbstractVerticle
import io.vertx.rxjava3.ext.web.Router
import io.vertx.rxjava3.ext.web.RoutingContext
import io.vertx.rxjava3.ext.web.handler.BodyHandler
import io.vertx.rxjava3.redis.client.Redis
import io.vertx.rxjava3.redis.client.RedisAPI
import io.vertx.rxjava3.redis.client.Response

class HttpServerVerticle : AbstractVerticle() {
    private lateinit var redisApi: RedisAPI

    override fun start(promise: Promise<Void>) {
        val redisOptions = RedisOptions()
            .setConnectionString("redis://:${Config.REDIS_PASSWORD}@${Config.REDIS_HOST}/${Config.REDIS_DATABASE}")

        Redis(io.vertx.redis.client.Redis.createClient(vertx.delegate, redisOptions))
            .rxConnect()
            .subscribe(
                { redisConnection ->
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
        var count = 0
        var dataSize = 0
        val users = ArrayList<JsonObject>()

        redisApi
            .rxKeys("heroes:*")
            .toObservable()
            .map { keys ->
                ArrayList<Observable<Response>>().apply {
                    keys.forEach {
                        add(redisApi
                            .rxHmget(ArrayList<String>().apply {
                                add(it.toString())
                                add("userId")
                                add("userName")
                                add("nameAlias")
                                add("company")
                            })
                            .toObservable())
                    }
                }
            }
            .flatMap {
                dataSize = it.size
                Observable.concat(it)
            }
            .subscribe(
                {
                    count++
                    val data = it.iterator()
                    users.add(JsonObject().apply {
                        put("userId", data.next().toString())
                        put("userName", data.next().toString())
                        put("nameAlias", data.next().toString())
                        put("company", data.next().toString())
                    })

                    if (count >= dataSize) {
                        response = JsonObject().apply {
                            put("success", true)
                            put("data", users)
                        }

                        putResponse(context, 200, response)
                    }
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("message", it.message)
                    }

                    putResponse(context, 500, response)
                })
    }

    private fun setUser(context: RoutingContext) {
        var response: JsonObject

        val userId = context.request().getParam("user_id")
        val userName = context.request().getParam("user_name")
        val nameAlias = context.request().getParam("name_alias")
        val company = context.request().getParam("company")

        redisApi
            .rxHmset(ArrayList<String>().apply {
                add("heroes:${userId}")
                add("userId")
                add(userId)
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

                            add("userId")
                            add(userId)

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

        redisApi
            .rxDel(ArrayList<String>().apply { add("heroes:$userId") })
            .subscribe(
                {
                    response = JsonObject().apply {
                        put("success", true)
                        put("action", "delete")
                    }

                    putResponse(context, 200, response)
                },
                {
                    response = JsonObject().apply {
                        put("success", false)
                        put("message", it.message)
                    }

                    putResponse(context, 500, response)

                })
    }

    private fun putResponse(context: RoutingContext, statuscode: Int, response: JsonObject) {
        context.response().statusCode = statuscode

        context.response().putHeader("Content-Type", "application/json")
        context.response().end(response.encode())
    }
}