package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class RepositoryTest {
    @Mock(answer = Answers.RETURNS_MOCKS) private QueryProvider mockQueryProvider;
    private Repository repository;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        repository = Repository.fromProvider(mockQueryProvider);
    }

    @Test
    public void testRepositoryQuery() {
        repository.entities(TestEntity.metaClass)
                .query()
                .where(TestEntity.$.key.eq(TestKey.create("aaa"))
                        .and(TestEntity.$.number.greaterThan(5))
                        .and(TestEntity.$.refEntity.text.contains("bbb")))
                .select(TestEntity.$.refEntities)
                .retrieve();
    }

    @Test
    public void testObserveAsList() {
        Subject<Notification<TestEntity>> notificationSubject = ReplaySubject.create();

        when(mockQueryProvider.aggregate(any(), any())).thenReturn(Maybe.just(0L));
        when(mockQueryProvider.query(any())).thenReturn(Observable.empty());
        when(mockQueryProvider.<TestKey, TestEntity, TestEntity>liveQuery(any())).thenReturn(notificationSubject);
        when(mockQueryProvider.<TestKey, TestEntity, TestEntity>queryAndObserve(any(), any()))
                .thenReturn(Observable
                        .just(Notification.<TestEntity>create(null, null, 1L))
                        .concatWith(notificationSubject));
        when(mockQueryProvider.<TestKey, TestEntity, TestEntity>queryAndObserve(any(), any()))
                .thenReturn(Observable
                .just(Notification.<TestEntity>create(null, null, 1L))
                .concatWith(notificationSubject));

        notificationSubject.onNext(Notification.ofCreated(TestEntities.testEntity1, 1L));

        TestObserver<List<TestEntity>> tester = repository.entities(TestEntity.metaClass)
                .query()
                .observeAsList()
                .doOnNext(l -> System.out.println("Received list of " + l.size()))
                .test()
                .awaitCount(1)
                .assertNoTimeout()
                .assertNoErrors()
                .assertValueAt(0, l -> l.size() == 1);

        notificationSubject.onNext(Notification.ofCreated(TestEntities.testEntity2, 1L));
        tester.awaitCount(2)
                .assertValueCount(2)
                .assertNoErrors()
                .assertNoTimeout()
                .assertValueAt(1, l -> l.size() == 2);
    }
}
