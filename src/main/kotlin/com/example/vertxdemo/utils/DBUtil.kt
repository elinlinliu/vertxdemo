package com.example.vertxdemo.utils

import io.vertx.core.json.JsonObject
import io.vertx.mysqlclient.MySQLConnectOptions
import io.vertx.mysqlclient.MySQLPool
import io.vertx.sqlclient.PoolOptions
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet
import io.vertx.kotlin.coroutines.awaitResult
import kotlin.collections.ArrayList
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.sql.ResultSet
import kotlin.io.println as println

class DBUtil {

    companion object {

        val client: MySQLPool

        init {
            val connectOptions: MySQLConnectOptions =
                MySQLConnectOptions().setHost("172.38.40.182").setPort(3306).setDatabase("dl_platform_old").setUser("root").setPassword("oc@2020")
            val poolOptions: PoolOptions = PoolOptions().setMaxSize(6)
            client = MySQLPool.pool(connectOptions, poolOptions)
            println("init mysql client: host:%s,port:%d,database:%s".format("172.38.40.182", 3306, "dl_platform_old"))
//            Runtime.getRuntime().addShutdownHook(Thread { client.close() })
        }

    }

    fun test() {
        client.query("select * from oc_bill limit 1").execute() { ar ->
            run {
                if (ar.succeeded()) {
                    val result: RowSet<Row> = ar.result()
                    result.forEach() {
                        println(it.toJson())
                    }
                } else {
                    println("ERROR: " + ar.cause().message);
                }
            }
        }
    }


    suspend fun queryAwaitResult(sql: String): ResultSet {
        return awaitResult {
            client.query(sql).execute()
            { ar ->
                if (ar.succeeded()) {
                    val toCollection = ar.result().mapNotNull { it.toJson() }.toCollection(ArrayList());
                } else {
                    println("ERROR: " + ar.cause().message);
                }
            }
        }

    }


    suspend fun query(sql: String): List<JsonObject> {
        var result = ArrayList<JsonObject>()
        val waiting = Channel<Boolean>() { }
        println("query start, " + Thread.currentThread().id)
        coroutineScope {
            launch {
                client.query(sql).execute() { ar ->
                    launch {
                        println("query end, " + Thread.currentThread().id)
                        if (ar.succeeded()) {
                            result = ar.result().mapNotNull { it.toJson() }.toCollection(ArrayList());
                        } else {
                            println("ERROR: " + ar.cause().message);
                        }
                        waiting.send(false)
                    }
                }
                waiting.receive()
            }
        }.join()
        return result
    }


    fun clear() {
        client.close()
    }


}

suspend fun main(args: Array<String>) {
    val dbUtil = DBUtil()
    println("main start, " + Thread.currentThread().id)
    val result = dbUtil.query("select * from oc_bill limit 1")
    println(result)
    println("main end, " + Thread.currentThread().id)
//    delay(10000)
}

