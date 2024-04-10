package io.github.jopenlibs.spring.dao.r2dbcpg

import io.github.jopenlibs.dbkqueue.config.QueueTableSchema
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.ConnectionFactory
import org.springframework.data.r2dbc.core.R2dbcEntityOperations
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer


/**
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
object PostgresDatabaseInitializer {
    const val DEFAULT_TABLE_NAME: String = "queue_default"
    const val DEFAULT_TABLE_NAME_WO_INC: String = "queue_default_wo_inc"
    const val CUSTOM_TABLE_NAME: String = "queue_custom"
    val DEFAULT_SCHEMA: QueueTableSchema = QueueTableSchema.builder().build()
    val CUSTOM_SCHEMA: QueueTableSchema = QueueTableSchema.builder()
        .withIdField("qid")
        .withQueueNameField("qn")
        .withPayloadField("pl")
        .withCreatedAtField("ct")
        .withNextProcessAtField("pt")
        .withAttemptField("at")
        .withReenqueueAttemptField("rat")
        .withTotalAttemptField("tat")
        .withExtFields(listOf("trace"))
        .build()

    private const val PG_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  qid      BIGSERIAL PRIMARY KEY,\n" +
            "  qn    TEXT NOT NULL,\n" +
            "  pl    TEXT,\n" +
            "  ct    TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  pt    TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  at    INTEGER                  DEFAULT 0,\n" +
            "  rat   INTEGER                  DEFAULT 0,\n" +
            "  tat   INTEGER                  DEFAULT 0,\n" +
            "  trace TEXT \n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (qn, pt, qid DESC);\n" +
            "\n"

    private const val PG_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n"

    private const val PG_DEFAULT_WO_INC_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGINT PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n"

    private lateinit var _r2dbcEntityOperations: R2dbcEntityOperations
    private lateinit var _transactionTemplate: TransactionTemplate
    private var initialized: Boolean = false

    val r2dbcEntityOperations: R2dbcEntityOperations
        get() {
            initialize()
            return _r2dbcEntityOperations
        }

    val transactionTemplate: TransactionTemplate
        get() {
            initialize()
            return _transactionTemplate
        }

    @Synchronized
    fun initialize() {
        if (initialized) {
            return
        }

//        val ryukImage: String = System.getProperty("testcontainers.ryuk.container.image") ?: "quay.io/testcontainers/ryuk:0.2.3"
//        TestcontainersConfiguration.getInstance().updateGlobalConfig("ryuk.container.image", ryukImage)

        val postgresImage: String = System.getProperty("testcontainers.postgresql.container.image") ?: "postgres:9.5"

        val dbContainer: PostgreSQLContainer<*> = PostgreSQLContainer(postgresImage)
        dbContainer.withEnv("POSTGRES_INITDB_ARGS", "--nosync")
        dbContainer.withCommand("postgres -c fsync=off -c full_page_writes=off -c synchronous_commit=off")
        dbContainer.start()

        var connectionFactory: ConnectionFactory = PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .host(dbContainer.host)
            .port(5432)
            .username(dbContainer.username)
            .password(dbContainer.password)
           .build()
        )

        _r2dbcEntityOperations = R2dbcEntityTemplate(connectionFactory)
        _transactionTemplate = TransactionTemplate(DataSourceTransactionManager())
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED)
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED)

        executeDdl("CREATE SEQUENCE tasks_seq START 1")
        createTable(PG_DEFAULT_WO_INC_TABLE_DDL, DEFAULT_TABLE_NAME_WO_INC)
        createTable(PG_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME)
        createTable(PG_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME)
    }

    fun createDefaultTable(tableName: String) {
        createTable(PG_DEFAULT_TABLE_DDL, tableName)
    }

    private fun createTable(ddlTemplate: String, tableName: String) {
        initialize()
        executeDdl(String.format(ddlTemplate, tableName, tableName, tableName))
    }

    private fun executeDdl(ddl: String) {
        initialize()
        transactionTemplate.execute<Any> { status: TransactionStatus ->
            r2dbcEntityOperations.databaseClient.sql(ddl)
                .fetch()
                .rowsUpdated()
                .block()
        }
    }
}
