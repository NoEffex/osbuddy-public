package com.rsbuddy.api;

import com.google.common.hash.Hashing;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.rsbuddy.api.db.LettuceManager;
import com.rsbuddy.api.protos.Counters;
import com.rsbuddy.api.protos.CountersServiceGrpc;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import io.lettuce.core.SetArgs;

import java.nio.charset.Charset;
import java.util.Base64;

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

    @Override
    public void queryPersonalBest(Counters.QueryPersonalBestRequest request, StreamObserver<Counters.QueryPersonalBestResponse> responseObserver) {
        Counters.QueryPersonalBestResponse response = lettuceManager.connection()
                .map(connection -> {
                    String boss = request.getBoss();
                    String displayName = request.getDisplayName();
                    if (boss == null || displayName == null) {
                        return INVALID_QUERY_PERSONAL_BEST_RESPONSE;
                    }
                    String key = personalBestKey(boss, displayName);
                    String valueString = connection.sync().get(key);
                    try {
                        if (valueString != null) {
                            return Counters.QueryPersonalBestResponse.newBuilder()
                                    .setSuccess(true)
                                    .setTime(Integer.parseInt(valueString))
                                    .build();
                        }
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                    return INVALID_QUERY_PERSONAL_BEST_RESPONSE;
                })
                .orElse(INVALID_QUERY_PERSONAL_BEST_RESPONSE);
        ServiceRequestContext.current().blockingTaskExecutor().submit(() -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }
    
    @Override
    public void putPersonalBest(Counters.PutPersonalBestRequest request, StreamObserver<Counters.PutPersonalBestResponse> responseObserver) {
        Counters.PutPersonalBestResponse response = lettuceManager.connection()
                .map(connection -> {
                    String boss = request.getBoss();
                    String displayName = request.getDisplayName();
                    if (boss == null || displayName == null) {
                        return INVALID_PUT_PERSONAL_BEST_RESPONSE;
                    }
                    String key = personalBestKey(boss, displayName);
                    String redisResponse = connection.sync().set(key, Integer.toString(request.getTime()), SetArgs.Builder.ex(ttlSeconds));
                    return "OK".equals(redisResponse) ? VALID_PUT_PERSONAL_BEST_RESPONSE : INVALID_PUT_PERSONAL_BEST_RESPONSE;
                })
                .orElse(INVALID_PUT_PERSONAL_BEST_RESPONSE);
        ServiceRequestContext.current().blockingTaskExecutor().submit(() -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void queryTask(Counters.QueryTaskRequest request, StreamObserver<Counters.QueryTaskResponse> responseObserver) {
        Counters.QueryTaskResponse response = lettuceManager.connection()
                .map(connection -> {
                    String displayName = request.getDisplayName();
                    if (displayName == null) {
                        return INVALID_QUERY_TASK_RESPONSE;
                    }
                    String key = taskKey(displayName);
                    String valueString = connection.sync().get(key);
                    try {
                        if (valueString != null) {
                            return Counters.QueryTaskResponse.newBuilder()
                                    .setSuccess(true)
                                    .setTask(Counters.Task.parseFrom(Base64.getDecoder().decode(valueString)))
                                    .build();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return INVALID_QUERY_TASK_RESPONSE;
                })
                .orElse(INVALID_QUERY_TASK_RESPONSE);
        ServiceRequestContext.current().blockingTaskExecutor().submit(() -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void putTask(Counters.PutTaskRequest request, StreamObserver<Counters.PutTaskResponse> responseObserver) {
        Counters.PutTaskResponse response = lettuceManager.connection()
                .map(connection -> {
                    String displayName = request.getDisplayName();
                    if (displayName == null) {
                        return INVALID_PUT_TASK_RESPONSE;
                    }
                    String key = taskKey(displayName);
                    byte[] data = request.getTask().toByteArray();
                    String redisResponse = connection.sync().set(key, Base64.getEncoder().encodeToString(data), SetArgs.Builder.ex(ttlSeconds));
                    return "OK".equals(redisResponse) ? VALID_PUT_TASK_RESPONSE : INVALID_PUT_TASK_RESPONSE;
                })
                .orElse(INVALID_PUT_TASK_RESPONSE);
        ServiceRequestContext.current().blockingTaskExecutor().submit(() -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    private String killCountKey(String boss, String displayName) {
        return Hashing.sha256().hashString("kc|" + boss + "|" + displayName.toLowerCase(), Charset.defaultCharset()).toString();
    }

    private String personalBestKey(String boss, String displayName) {
        return Hashing.sha256().hashString("pb|" + boss + "|" + displayName.toLowerCase(), Charset.defaultCharset()).toString();
    }

    private String taskKey(String displayName) {
        return Hashing.sha256().hashString("task|" + displayName.toLowerCase(), Charset.defaultCharset()).toString();
    }

    private static final Counters.QueryKillCountResponse INVALID_QUERY_KILL_COUNT_RESPONSE = Counters.QueryKillCountResponse.newBuilder()
            .setSuccess(false)
            .build();
    private static final Counters.PutKillCountResponse VALID_PUT_KILL_COUNT_RESPONSE = Counters.PutKillCountResponse.newBuilder()
            .setSuccess(true)
            .build();
    private static final Counters.PutKillCountResponse INVALID_PUT_KILL_COUNT_RESPONSE = Counters.PutKillCountResponse.newBuilder()
            .setSuccess(false)
            .build();
    private static final Counters.QueryPersonalBestResponse INVALID_QUERY_PERSONAL_BEST_RESPONSE = Counters.QueryPersonalBestResponse.newBuilder()
            .setSuccess(false)
            .build();
    private static final Counters.PutPersonalBestResponse VALID_PUT_PERSONAL_BEST_RESPONSE = Counters.PutPersonalBestResponse.newBuilder()
            .setSuccess(true)
            .build();
    private static final Counters.PutPersonalBestResponse INVALID_PUT_PERSONAL_BEST_RESPONSE = Counters.PutPersonalBestResponse.newBuilder()
            .setSuccess(false)
            .build();
    private static final Counters.QueryTaskResponse INVALID_QUERY_TASK_RESPONSE = Counters.QueryTaskResponse.newBuilder()
            .setSuccess(false)
            .build();
    private static final Counters.PutTaskResponse VALID_PUT_TASK_RESPONSE = Counters.PutTaskResponse.newBuilder()
            .setSuccess(true)
            .build();
    private static final Counters.PutTaskResponse INVALID_PUT_TASK_RESPONSE = Counters.PutTaskResponse.newBuilder()
            .setSuccess(false)
            .build();
}
