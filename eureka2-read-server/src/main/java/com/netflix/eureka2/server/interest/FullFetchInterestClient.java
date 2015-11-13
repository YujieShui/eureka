package com.netflix.eureka2.server.interest;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.eureka2.channel.ChannelFactory;
import com.netflix.eureka2.channel.InterestChannel;
import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.client.interest.AbstractInterestClient;
import com.netflix.eureka2.connection.RetryableConnection;
import com.netflix.eureka2.connection.RetryableConnectionFactory;
import com.netflix.eureka2.health.AbstractHealthStatusProvider;
import com.netflix.eureka2.health.HealthStatusProvider;
import com.netflix.eureka2.health.HealthStatusUpdate;
import com.netflix.eureka2.health.SubsystemDescriptor;
import com.netflix.eureka2.model.interest.Interest;
import com.netflix.eureka2.model.interest.Interests;
import com.netflix.eureka2.model.instance.InstanceInfo;
import com.netflix.eureka2.model.instance.InstanceInfo.Status;
import com.netflix.eureka2.model.notification.ChangeNotification;
import com.netflix.eureka2.model.notification.ChangeNotification.Kind;
import com.netflix.eureka2.registry.EurekaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

/**
 * {@link EurekaInterestClient} implementation with single full registry fetch subscription.
 * Client interest subscriptions are not propagated to the channel, as the registry is already eagerly
 * subscribed to the full content.
 *
 * @author Tomasz Bak
 */
public class FullFetchInterestClient extends AbstractInterestClient implements HealthStatusProvider<FullFetchInterestClient> {

    private static final Logger logger = LoggerFactory.getLogger(FullFetchInterestClient.class);

    private static final SubsystemDescriptor<FullFetchInterestClient> DESCRIPTOR = new SubsystemDescriptor<>(
            FullFetchInterestClient.class,
            "Read Server full fetch InterestClient",
            "Source of registry data for Eureka read server clients."
    );

    private final RetryableConnection<InterestChannel> retryableConnection;
    private final FullFetchInterestClientHealth healthProvider;

    @Inject
    public FullFetchInterestClient(EurekaRegistry<InstanceInfo> registry,
                                   ChannelFactory<InterestChannel> channelFactory) {
        this(registry, channelFactory, DEFAULT_RETRY_WAIT_MILLIS);
    }

    /* visible for testing*/ FullFetchInterestClient(final EurekaRegistry<InstanceInfo> registry,
                                                     ChannelFactory<InterestChannel> channelFactory,
                                                     int retryWaitMillis) {
        super(registry, retryWaitMillis, Schedulers.computation());

        this.healthProvider = new FullFetchInterestClientHealth();

        RetryableConnectionFactory<InterestChannel> retryableConnectionFactory
                = new RetryableConnectionFactory<>(channelFactory);

        Func1<InterestChannel, Observable<Void>> executeOnChannel = new Func1<InterestChannel, Observable<Void>>() {
            @Override
            public Observable<Void> call(InterestChannel interestChannel) {
                return interestChannel.change(Interests.forFullRegistry());
            }
        };

        this.retryableConnection = retryableConnectionFactory.zeroOpConnection(executeOnChannel);

        lifecycleSubscribe(retryableConnection);
        bootstrapUploadSubscribe();
    }

    @Override
    public Observable<ChangeNotification<InstanceInfo>> forInterest(final Interest<InstanceInfo> interest) {
        if (isShutdown.get()) {
            return Observable.error(new IllegalStateException("InterestHandler has shutdown"));
        }
        return registry.forInterest(interest);
    }

    @Override
    protected RetryableConnection<InterestChannel> getRetryableConnection() {
        return retryableConnection;
    }

    @Override
    public void shutdown() {
        healthProvider.moveHealthTo(Status.DOWN);
        super.shutdown();
    }

    @Override
    public Observable<HealthStatusUpdate<FullFetchInterestClient>> healthStatus() {
        return healthProvider.healthStatus();
    }

    /**
     * Eureka Read server registry is ready when the initial batch of data is uploaded from the server.
     */
    private void bootstrapUploadSubscribe() {
        final AtomicInteger boostrapCount = new AtomicInteger(0);
        forInterest(Interests.forFullRegistry()).takeWhile(new Func1<ChangeNotification<InstanceInfo>, Boolean>() {
            @Override
            public Boolean call(ChangeNotification<InstanceInfo> notification) {
                boostrapCount.incrementAndGet();
                return notification.getKind() != Kind.BufferSentinel;
            }
        }).doOnCompleted(new Action0() {
            @Override
            public void call() {
                healthProvider.moveHealthTo(Status.UP);
                logger.info("Initial bootstrap completed. Bootstrapped with {} instances", boostrapCount.get());
            }
        }).subscribe();
    }

    public static class FullFetchInterestClientHealth extends AbstractHealthStatusProvider<FullFetchInterestClient> {

        protected FullFetchInterestClientHealth() {
            super(Status.STARTING, DESCRIPTOR);
        }

    }
}