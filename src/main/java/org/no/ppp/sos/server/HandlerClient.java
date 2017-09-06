package org.no.ppp.sos.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.no.ppp.sos.model.Packet;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
        b = new Bootstrap().channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true).group(workerGroup)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline channelPipeline = ch.pipeline();
                        channelPipeline.addLast(createChannelHandler());
                    }
                });
    }

    @Override
    protected void onRemotePacket(Packet packet) {
        if (packet.isOpen()) {
            ChannelFuture channelFuture = b.connect(host, port);

            Channel channel = channelFuture.channel();
            channel.attr(A_I).set(true);

            setChannelId(channel, packet.getId()); // a trick

            if (logger.isInfoEnabled()) {
                logger.info("Connecting to {}:{} ...", host, port);
            }
            try {
                channelFuture.sync();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            if (logger.isInfoEnabled()) {
                logger.info("Connected");
            }
            return;
        }

        String id = packet.getId();

        Channel channel = channels.get(id);
        if (channel == null) {
            logger.warn("Channel is no longer manageable: {}", id);
            return;
        }

        if (packet.isClose()) {
            channel.attr(A_I).set(true);
            channel.close();
            return;
        }

        if (packet.getData() != null) {
            channel.writeAndFlush(Unpooled.wrappedBuffer(packet.getData()));
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
