package io.github.jopenlibs.dbkqueue.stub

import io.github.jopenlibs.dbkqueue.settings.ExtSettings
import io.github.jopenlibs.dbkqueue.settings.FailRetryType
import io.github.jopenlibs.dbkqueue.settings.FailureSettings
import io.github.jopenlibs.dbkqueue.settings.PollSettings
import io.github.jopenlibs.dbkqueue.settings.ProcessingMode
import io.github.jopenlibs.dbkqueue.settings.ProcessingSettings
import io.github.jopenlibs.dbkqueue.settings.QueueSettings
import io.github.jopenlibs.dbkqueue.settings.ReenqueueRetryType
import io.github.jopenlibs.dbkqueue.settings.ReenqueueSettings
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
