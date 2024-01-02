package com.slimgears.rxrepo.orientdb;

import com.google.auto.value.AutoValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.rxrepo.sql.SqlFields;
import io.reactivex.ObservableEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.slimgears.util.generic.MoreStrings.lazy;

class OrientDbLiveQueryListener implements OLiveQueryResultListener {
    private final ObservableEmitter<LiveQueryNotification> emitter;
    private final static Logger log = LoggerFactory.getLogger(OrientDbLiveQueryListener.class);

    OrientDbLiveQueryListener(ObservableEmitter<LiveQueryNotification> emitter) {
        this.emitter = emitter;
    }

    @AutoValue
    public static abstract class LiveQueryNotification {
        public abstract ODatabaseDocument database();
        @Nullable public abstract OResult oldResult();
        @Nullable public abstract OResult newResult();
        @Nullable public abstract Long sequenceNumber();

        public static LiveQueryNotification create(ODatabaseDocument db, OResult oldRes, OResult newRes, Long sequenceNumber) {
            return new AutoValue_OrientDbLiveQueryListener_LiveQueryNotification(db, oldRes, newRes, sequenceNumber);
        }
    }

    @Override
    public void onCreate(ODatabaseDocument database, OResult data) {
        log.trace("onCreate Notification received: {}", lazy(data::toJSON));
        log.trace("Beginning emit >>");
        emitter.onNext(LiveQueryNotification.create(database, null, data, data.getProperty(SqlFields.sequenceFieldName)));
        log.trace("Emit finished <<");
    }

    @Override
    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
        log.trace("onUpdate Notification received: {} -> {}", lazy(before::toJSON), lazy(after::toJSON));
        log.trace("Beginning emit >>");
        emitter.onNext(LiveQueryNotification.create(database, before, after, after.getProperty(SqlFields.sequenceFieldName)));
        log.trace("Emit finished <<");
    }

    @Override
    public void onDelete(ODatabaseDocument database, OResult data) {
        log.trace("onDeleted Notification received: {}", lazy(data::toJSON));
        log.trace("Beginning emit >>");
        emitter.onNext(LiveQueryNotification.create(database, data, null, data.getProperty(SqlFields.sequenceFieldName)));
        log.trace("Emit finished <<");
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {
        log.error("onError notification received", exception);
        log.trace("Beginning emit >>");
        emitter.onError(exception);
        log.trace("Emit finished <<");
    }

    @Override
    public void onEnd(ODatabaseDocument database) {
        emitter.onComplete();
    }
}
