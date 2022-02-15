import io.vertx.core.Launcher
import verticle.MainVerticle

fun main() {
    Launcher.executeCommand("run", MainVerticle::class.java.name)
}