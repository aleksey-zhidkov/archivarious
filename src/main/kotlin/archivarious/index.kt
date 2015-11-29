package archivarious

import org.h2.jdbc.JdbcSQLException
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import java.sql.DriverManager
import java.sql.Timestamp
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    Class.forName("org.h2.Driver")
    val path = "~/tmp/arch/test"
    with(File(path)) {
        mkdirs()
    }
    val conn = DriverManager.
            getConnection("jdbc:h2:$path", "arch", "arch")
    try {
        with(conn.createStatement()) {
            try {
                execute("""CREATE TABLE nodes (
          dir VARCHAR(4096),
          name VARCHAR(1024),
          hash VARCHAR(64),
          last_modified TIMESTAMP,
          is_directory BOOL,
          length INT8
        )""")
                close()
            } catch(e: JdbcSQLException) {
                System.err.println(e.toString())
            }
        }
        val hashedNodes = with(conn.createStatement()) {
            val rs = executeQuery("SELECT dir, name, last_modified FROM nodes")
            val hashedNodes = hashMapOf<String, Long>()
            while (rs.next()) {
                val key = rs.getString(1) + "/" + rs.getString(2)
                val value = rs.getTimestamp(3).toInstant().epochSecond
                hashedNodes.put(key, value)
            }
            close()
            hashedNodes
        }
        val insert = conn.prepareStatement("INSERT INTO nodes (dir, name, hash, last_modified, is_directory, length) VALUES (?, ?, ?, ?, ?, ?)")
        val nodes = index(Paths.get("/data/yandex-disk"), hashedNodes)
        //val nodes = index(Paths.get("/data/yandex-disk/Archive/Марина/Marina sorted/Фото и Видео"), hashedNodes)
        nodes.forEach {
            val (node, hash) = it
            insert.setString(1, node.parent.toString())
            insert.setString(2, node.toFile().name)
            insert.setString(3, hash)
            insert.setTimestamp(4, Timestamp(node.toFile().lastModified()))
            insert.setBoolean(5, node.toFile().isDirectory)
            insert.setLong(6, node.toFile().length())
            insert.execute()
        }
        insert.close()

    } finally {
        conn.close()
    }
}

var bytes = 0L

var files = 0
var totalFiles = 0
val start = System.currentTimeMillis()
fun index(base: Path, hashedNodes: HashMap<String, Long>): List<Pair<Path, String>> {
    fun indexNode(node: Path): List<Pair<Path, String>> {
        val file = node.toFile()
        return when {
            file.isDirectory -> {
                file.list().flatMap { indexNode(node.resolve(it)) }
            }
            else -> {
                if (!file.exists() || hashedNodes[file.absolutePath] == file.lastModified()) return emptyList()
                val digester = MessageDigest.getInstance("SHA-256")
                file.forEachBlock({ buffer, len -> bytes += len;digester.update(buffer, 0, len)}, Math.min(file.length(), 128L * 1024 * 1024).toInt())
                val hash = digester.digest().map { "%02x".format(it.toInt() and 0xFF) }.joinToString(separator = "")
                files++
                totalFiles++
                if (files % 10 == 0 && System.currentTimeMillis() - start > 1000)
                    println ("$files/$totalFiles: $hash / ${file.absoluteFile} $bytes ${bytes / ((System.currentTimeMillis() - start) / 1000)} B/s")
                return listOf(Pair(node, hash))
            }
        }
    }
    return indexNode(base)
}