package com.rsbuddy.counters;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.rsbuddy.counters.db.LettuceManager;
import com.rsbuddy.counters.protos.Counters;
import com.rsbuddy.counters.protos.CountersServiceGrpc;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import io.lettuce.core.SetArgs;

public class CountersServiceImpl extends CountersServiceGrpc.CountersServiceImplBase {
    private final LettuceManager lettuceManager;
    private final int ttlSeconds;

    public CountersServiceImpl(LettuceManager lettuceManager, Config config) {
        this.lettuceManager = lettuceManager;
        this.ttlSeconds = config.getInt("ttlSeconds");
    }

    @Override
    public void queryKillCount(Counters.QueryKillCountRequest request, StreamObserver<Counters.QueryKillCountResponse> responseObserver) {
        Counters.QueryKillCountResponse response = lettuceManager.connection()
                .map(connection -> {
                    String boss = request.getBoss();
                    String displayName = request.getDisplayName();
                    if (boss == null || displayName == null) {
                        return INVALID_QUERY_KILL_COUNT_RESPONSE;
                    }
                    String key = killCountKey(boss, displayName);
                    String valueString = connection.sync().get(key);
                    try {
                        if (valueString != null) {
                            return Counters.QueryKillCountResponse.newBuilder()
                                    .setSuccess(true)
                                    .setCount(Integer.parseInt(valueString))
                                    .build();
                        }
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                    return INVALID_QUERY_KILL_COUNT_RESPONSE;
                })
                .orElse(INVALID_QUERY_KILL_COUNT_RESPONSE);
        ServiceRequestContext.current().blockingTaskExecutor().submit(() -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void putKillCount(Counters.PutKillCountRequest request, StreamObserver<Counters.PutKillCountResponse> responseObserver) {
        Counters.PutKillCountResponse response = lettuceManager.connection()
                .map(connection -> {
                    String boss = request.getBoss();
                    String displayName = request.getDisplayName();
                    if (boss == null || displayName == null) {
                        return INVALID_PUT_KILL_COUNT_RESPONSE;
                    }
                    String key = killCountKey(boss, displayName);
                    String redisResponse = connection.sync().set(key, Integer.toString(request.getCount()), SetArgs.Builder.ex(ttlSeconds));
                    return "OK".equals(redisResponse) ? VALID_PUT_KILL_COUNT_RESPONSE : INVALID_PUT_KILL_COUNT_RESPONSE;
                })
                .orElse(INVALID_PUT_KILL_COUNT_RESPONSE);
        ServiceRequestContext.current().blockingTaskExecutor().submit(() -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    private String killCountKey(String boss, String displayName) {
        return "kc|" + boss + "|" + displayName.toLowerCase();
    }

    private static final Counters.QueryKillCountResponse INVALID_QUERY_KILL_COUNT_RESPONSE = Counters.QueryKillCountResponse.newBuilder()
            .setSuccess(false)
            .build();

    private static final Counters.PutKillCountResponse INVALID_PUT_KILL_COUNT_RESPONSE = Counters.PutKillCountResponse.newBuilder()
            .setSuccess(false)
            .build();

    private static final Counters.PutKillCountResponse VALID_PUT_KILL_COUNT_RESPONSE = Counters.PutKillCountResponse.newBuilder()
            .setSuccess(true)
            .build();
}
