package org.no.ppp.sos.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.no.ppp.sos.model.Packet;
import org.no.ppp.sos.model.Packet.Type;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class HandlerServer extends HandlerBase {

    private String host;

    private Integer port;

    private EventLoopGroup parentGroup;

    private EventLoopGroup workerGroup;

    private ServerBootstrap serverBootstrap;

    private Channel channel;

    public HandlerServer(InputStream is, OutputStream os, String host, Integer port) throws IOException {
        super(is, os);

        this.host = host;
        this.port = port;

        parentGroup = new NioEventLoopGroup(8);
        workerGroup = new NioEventLoopGroup(8);
        serverBootstrap = new ServerBootstrap().channel(NioServerSocketChannel.class)
            .group(parentGroup, workerGroup).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline channelPipeline = ch.pipeline();
                    channelPipeline.addLast(createChannelHandler());
                }
            });
    }

    @Override
    protected void onChannelOpen(ChannelContext channelContext) {
        outgoingPacketQueue.offer(new Packet(channelContext.getId()).setType(Type.OPEN));
    }

    @Override
    protected void onData(Packet p) {
        if (p.getId().equals("init")) {
            onDeferredStart();
            return;
        }
        super.onData(p);
    }

    protected void onDeferredStart() {
        try {
            channel = serverBootstrap.bind(host, port)
                    .sync().channel().closeFuture().channel();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        if (logger.isInfoEnabled()) {
            logger.info("Netty server started: host={}, port={}", host, port);
        }
    }

    @Override
    public void onStop() throws InterruptedException {
        channel.close().sync();

        parentGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();

        if (logger.isInfoEnabled()) {
            logger.info("Netty server stopped");
        }
    }

}
