package io.github.jopenlibs.spring.dao.r2dbcpg

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer

/**
 */
class Dbg  {

    companion object {
        @JvmStatic
        @BeforeAll
        fun beforeAll() {
        }
    }
    @Test
    fun dbg() {
        val postgresImage: String = System.getProperty("testcontainers.postgresql.container.image") ?: "postgres:latest"

        val dbContainer: PostgreSQLContainer<*> = PostgreSQLContainer(postgresImage)
        dbContainer.withEnv("POSTGRES_INITDB_ARGS", "--nosync")
        dbContainer.withCommand("postgres -c fsync=off -c full_page_writes=off -c synchronous_commit=off")
        dbContainer.start()
    }
}
