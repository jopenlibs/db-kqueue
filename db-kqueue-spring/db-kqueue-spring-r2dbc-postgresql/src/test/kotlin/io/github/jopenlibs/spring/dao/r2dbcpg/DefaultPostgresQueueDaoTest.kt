package io.github.jopenlibs.spring.dao.r2dbcpg

import org.junit.jupiter.api.BeforeAll

/**
 */
object DefaultPostgresQueueDaoTest : QueueDaoTest(
    PostgresQueueDao(
        PostgresDatabaseInitializer.r2dbcEntityOperations,
        PostgresDatabaseInitializer.DEFAULT_SCHEMA,
    ),
    PostgresDatabaseInitializer.DEFAULT_TABLE_NAME,
    PostgresDatabaseInitializer.DEFAULT_SCHEMA,
    PostgresDatabaseInitializer.r2dbcEntityOperations,
    PostgresDatabaseInitializer.transactionTemplate,
) {
    @JvmStatic
    @BeforeAll
    fun beforeAll() {
        PostgresDatabaseInitializer.initialize()
    }
}
