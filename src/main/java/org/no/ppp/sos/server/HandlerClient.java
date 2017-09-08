package org.no.ppp.sos.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.no.ppp.sos.model.Packet;
import org.no.ppp.sos.util.CustomChannelId;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class HandlerClient extends HandlerBase {

    private String host;

    private Integer port;

    private EventLoopGroup workerGroup;

    private Bootstrap b;

    public HandlerClient(InputStream is, OutputStream os, String host, Integer port) throws IOException {
        super(is, os);

        this.host = host;
        this.port = port;

        workerGroup = new NioEventLoopGroup(8);
        b = new Bootstrap().channelFactory((ChannelFactory<?>) () -> new NioSocketChannel() {
            @Override
            protected ChannelId newId() {
                try {
                    if (remoteChannelId == null) {
                        throw new IllegalStateException();
                    }
                    return new CustomChannelId(remoteChannelId);
                } finally {
                    remoteChannelId = null;
                }
            }
        }).option(ChannelOption.SO_KEEPALIVE, true).group(workerGroup)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline channelPipeline = ch.pipeline();
                    channelPipeline.addLast(createChannelHandler());
                }
            });
    }

    private String remoteChannelId;

    @Override
    protected synchronized void onOpen(Packet p) {
        remoteChannelId = p.getId();

        ChannelFuture channelFuture = b.connect(host, port);

        if (logger.isInfoEnabled()) {
            logger.info("Connecting to {}:{} ...", host, port);
        }
        try {
            channelFuture.sync();
            channelFuture.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).sync();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Connected");
        }
    }

    @Override
    protected void onStart() {
        if (logger.isInfoEnabled()) {
            logger.info("Netty client is targeted on: {}:{}", host, port);
        }
        outgoingPacketQueue.offer(new Packet("init"));
    }

}
