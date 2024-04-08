package io.github.jopenlibs.dbkqueue.stub

import ru.yoomoney.tech.dbqueue.settings.ExtSettings
import ru.yoomoney.tech.dbqueue.settings.FailRetryType
import ru.yoomoney.tech.dbqueue.settings.FailureSettings
import ru.yoomoney.tech.dbqueue.settings.PollSettings
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings
import ru.yoomoney.tech.dbqueue.settings.QueueSettings
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings
import java.time.Duration

object TestFixtures {
    fun createQueueSettings(): QueueSettings.Builder {
        return QueueSettings.builder()
            .withProcessingSettings(createProcessingSettings().build())
            .withPollSettings(createPollSettings().build())
            .withFailureSettings(createFailureSettings().build())
            .withReenqueueSettings(createReenqueueSettings().build())
            .withExtSettings(ExtSettings.builder().withSettings(HashMap()).build())
    }

    fun createProcessingSettings(): ProcessingSettings.Builder {
        return ProcessingSettings.builder()
            .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
            .withThreadCount(1)
    }

    fun createPollSettings(): PollSettings.Builder {
        return PollSettings.builder()
            .withBetweenTaskTimeout(Duration.ofMillis(0))
            .withNoTaskTimeout(Duration.ofMillis(0))
            .withFatalCrashTimeout(Duration.ofSeconds(0))
    }

    fun createFailureSettings(): FailureSettings.Builder {
        return FailureSettings.builder()
            .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
            .withRetryInterval(Duration.ofMinutes(1))
    }

    fun createReenqueueSettings(): ReenqueueSettings.Builder {
        return ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.MANUAL)
    }
}
