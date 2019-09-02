package com.rsbuddy.counters;

import java.net.InetSocketAddress;

import com.rsbuddy.counters.db.LettuceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.common.grpc.GrpcSerializationFormats;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocServiceBuilder;
import com.linecorp.armeria.server.docs.DocServiceFilter;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;

import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load().resolve();
        Server server = newServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop().join();
            logger.info("Server has been stopped.");
        }));

        server.start().join();
        InetSocketAddress localAddress = server.activePort().get().localAddress();
        boolean isLocalAddress = localAddress.getAddress().isAnyLocalAddress() ||
                localAddress.getAddress().isLoopbackAddress();
        logger.info("Server has been started. Serving DocService at http://{}:{}/docs",
                isLocalAddress ? "127.0.0.1" : localAddress.getHostString(), localAddress.getPort());
    }

    static Server newServer(Config config) throws Exception {
        LettuceManager lettuceManager = new LettuceManager(config.getConfig("redis"));
        ServerBuilder builder = new ServerBuilder()
                .http(config.getInt("httpPort"))
                .service(new GrpcServiceBuilder()
                        .addService(new CountersServiceImpl(lettuceManager, config))
                        // See https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md
                        .addService(ProtoReflectionService.newInstance())
                        .supportedSerializationFormats(GrpcSerializationFormats.values())
                        .enableUnframedRequests(true)
                        .build());
        if (config.getBoolean("publicDocumentation")) {
            builder.serviceUnder("/docs", new DocServiceBuilder()
                    .exclude(DocServiceFilter.ofServiceName(ServerReflectionGrpc.SERVICE_NAME))
                    .build());
        }
        return builder.build();
    }

}
