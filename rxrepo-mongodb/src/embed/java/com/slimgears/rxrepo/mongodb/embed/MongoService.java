package com.slimgears.rxrepo.mongodb.embed;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Feature;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.config.RuntimeConfig;
import de.flapdoodle.embed.process.config.store.DownloadConfig;
import de.flapdoodle.embed.process.distribution.Version;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.progress.ProgressListener;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("WeakerAccess")
public class MongoService implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(MongoService.class);
    private final static String host = "localhost";
    private final int port;
    private final String version;
    private final String connectionString;
    private final boolean enableReplica;
    private final AtomicReference<MongodProcess> process = new AtomicReference<>();
    private final MongodStarter starter;

    private MongoService(int port, String version, String workingDir, boolean enableReplica) {
        this.port = port;
        this.version = version;
        this.connectionString = "mongodb://" + host + ":" + port;
        workingDir = Optional.ofNullable(workingDir).filter(wd -> !wd.isEmpty()).orElse(".") + "/.mongo";
        this.enableReplica = enableReplica;

        DownloadConfig downloadConfig = Defaults.downloadConfigFor(Command.MongoD)
                .artifactStorePath(new FixedPath(workingDir + "/dist"))
                .progressListener(new LoggingProgressListener(10))
                .build();

        RuntimeConfig config = Defaults.runtimeConfigFor(Command.MongoD)
                .artifactStore(Defaults.extractedArtifactStoreFor(Command.MongoD).withDownloadConfig(downloadConfig))
                .build();

        this.starter = MongodStarter.getInstance(config);
    }

    public MongoService start() {
        try {
            ImmutableMongodConfig.Builder mongodConfigBuilder = MongodConfig.builder()
                    .version(Versions.withFeatures(Version.of(version), Feature.SYNC_DELAY, Feature.STORAGE_ENGINE, Feature.ONLY_64BIT, Feature.NO_CHUNKSIZE_ARG, Feature.MONGOS_CONFIGDB_SET_STYLE, Feature.NO_HTTP_INTERFACE_ARG, Feature.ONLY_WITH_SSL, Feature.ONLY_WINDOWS_2008_SERVER, Feature.NO_SOLARIS_SUPPORT, Feature.NO_BIND_IP_TO_LOCALHOST))
                    .net(new Net(host, port, false))
                    .cmdOptions(MongoCmdOptions.builder().useNoJournal(false).build())
                    .timeout(new Timeout(10000))
                    .stopTimeoutInMillis(10000);

            if (enableReplica) {
                mongodConfigBuilder.replication(new Storage(null, "rs0", 5000));
            }

            MongodConfig mongodConfig = mongodConfigBuilder.build();

            MongodExecutable executable = starter.prepare(mongodConfig);
            MongodProcess process = executable.start();
            this.process.set(process);

            if (enableReplica) {
                try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                    MongoDatabase adminDb = mongoClient.getDatabase("admin");
                    Document rsConfig = new Document("_id", "rs0")
                            .append("members", Collections.singletonList(
                                    new Document("_id", 0)
                                            .append("host", host + ":" + port)));

                    Completable.fromPublisher(adminDb.runCommand(new Document("replSetInitiate", rsConfig)))
                            .blockingAwait();

                    Observable.fromPublisher(adminDb.runCommand(new Document("replSetGetStatus", 1)))
                            .ignoreElements()
                            .blockingAwait();
                }
            }
        } catch (IOException e) {
            Optional.ofNullable(this.process.get()).ifPresent(MongodProcess::stop);
            throw new RuntimeException(e);
        }

        return this;
    }

    public void stop() {
        MongodProcess process = this.process.getAndSet(null);
        if (process == null) {
            return;
        }

        System.out.println("Stopping mongod...");
        process.stop();
        //noinspection ResultOfMethodCallIgnored
        Observable.interval(200, TimeUnit.MILLISECONDS)
                .takeWhile(i -> process.isProcessRunning())
                .ignoreElements()
                .blockingAwait(2000, TimeUnit.MILLISECONDS);

        System.out.println("Done");
    }

    @Override
    public void close() {
        stop();
    }

    private static class LoggingProgressListener implements ProgressListener {
        private final AtomicInteger lastPercent = new AtomicInteger();
        private final int progressGranularity;

        private LoggingProgressListener(int progressGranularity) {
            this.progressGranularity = progressGranularity;
        }

        @Override
        public void progress(String label, int percent) {
            int roundPercent = Math.round(percent / progressGranularity) * progressGranularity;
            if (lastPercent.get() != roundPercent) {
                log.info("{} download progress: {}% done", label, roundPercent);
                lastPercent.set(roundPercent);
            }
        }

        @Override
        public void done(String label) {
            log.info("Finished downloading {}", label);
        }

        @Override
        public void start(String label) {
            log.info("Starting download: {}", label);
        }

        @Override
        public void info(String label, String message) {
            log.info("{}: {}", label, message);
        }
    }

    public static class Builder {
        private int port = 27017;
        private String version = "4.0.26";
        private String workingDir = "";
        private boolean enableReplica = false;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder workingDir(String workingDir) {
            this.workingDir = workingDir;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder enableReplica() {
            return enableReplica(true);
        }

        public Builder enableReplica(boolean enable) {
            this.enableReplica = enable;
            return this;
        }

        public MongoService build() {
            return new MongoService(port, version, workingDir, enableReplica);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
