package org.no.ppp.sos.server;

import static io.netty.buffer.ByteBufUtil.getBytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.no.ppp.sos.model.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.util.AttributeKey;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

public abstract class HandlerBase {

    /**
     * Indicates a stream incoming initiative.
     */
    protected static final AttributeKey<Boolean> A_I = AttributeKey.valueOf("I");

    protected BlockingQueue<Packet> outgoingPacketQueue = new ArrayBlockingQueue<>(20);

    private Thread incoming;

    private Thread outgoing;

    protected HandlerBase(InputStream is, OutputStream os) throws IOException {

        incoming = new Thread(() -> {
            logger.info("Pump task started.");
            try {
                startStreamToQueuePump(is, this::onRemotePacket);
            } catch (Exception e) {
                logger.warn("Pump task was interrupted by exception", e);
                throw new RuntimeException(e);
            }
            logger.info("Pump task finished.");
        });
        incoming.setName("incoming");

        outgoing = new Thread(() -> {
            logger.info("Pump task started.");
            try {
                startQueueToStreamPump(outgoingPacketQueue, os);
            } catch (Exception e) {
                logger.warn("Pump task was interrupted by exception", e);
                throw new RuntimeException(e);
            }
            logger.info("Pump task finished.");
        });
        outgoing.setName("outgoing");
    }

    protected void startStreamToQueuePump(InputStream stream, Consumer<Packet> consumer) throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        while (true) {
            int b;
            while ((b = stream.read()) != -1) {
                if (b == '\r' || b == '\n') {
                    if (buffer.readableBytes() == 0) {
                        continue;
                    }
                    byte[] bytes = ByteBufUtil.getBytes(buffer);
                    buffer.clear();
                    Packet packet = Packet.of(bytes);
                    if (logger.isInfoEnabled()) {
                        logger.info("<== " + packet);
                    }
                    consumer.accept(packet);
                } else {
                    buffer.writeByte(b);
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    protected void startQueueToStreamPump(BlockingQueue<Packet> queue, OutputStream stream) {
        try (PrintWriter pw = new PrintWriter(stream)) {
            while (true) {
                Packet packet;
                try {
                    packet = queue.take();
                } catch (InterruptedException e) {
                    break;
                }
                if (logger.isInfoEnabled()) {
                    logger.info("==> " + packet);
                }
                pw.println(packet.asString());
                pw.flush();
            }
        }
    }

    /**
     * Channel local dictionary.
     */
    private Map<ChannelId, String> ids = new HashMap<>();

    protected String getChannelId(Channel channel) {
        return ids.get(channel.id());
    }

    protected String setChannelId(Channel channel, String id) {
        return ids.put(channel.id(), id);
    }

    protected Map<String, Channel> channels = new ConcurrentHashMap<>();

    protected ChannelDuplexHandler createChannelHandler() {
        return new ChannelDuplexHandler() {

            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                Channel channel = ctx.channel();

                String id = getChannelId(channel);

                if (logger.isInfoEnabled()) {
                    logger.info("Engage channel: {}", id);
                }
                if (channels.put(id, channel) != null) {
                    throw new IllegalStateException();
                }

                // send open socket packet
                if (channel.attr(A_I).get() == null) {
                    outgoingPacketQueue.offer(new Packet(id).setOpen(true));
                }
                channel.attr(A_I).set(null);
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                Channel channel = ctx.channel();
                String id = getChannelId(channel);
                if (logger.isInfoEnabled()) {
                    logger.info("Disarm channel: {}", id);
                }

                // send close socket packet on channel close
                if (channel.attr(A_I).get() == null) {
                    outgoingPacketQueue.offer(new Packet(id).setClose(true));
                }
                channel.attr(A_I).set(null);

                channels.remove(id);
                ids.remove(channel.id());
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                String id = getChannelId(ctx.channel());
                outgoingPacketQueue.offer(new Packet(id).setData(getBytes((ByteBuf) msg)));
            }
        };
    }

    public void start() throws InterruptedException {
        logger.info("Netty starting ...");

        incoming.start();
        outgoing.start();

        onStart();
    }

    public void stop() throws InterruptedException {
        logger.info("Netty is being stopped");

        incoming.interrupt();

        channels.values().forEach(c -> c.close());

        outgoing.interrupt();

        onStop();
    }

    protected void onStart() {}

    protected void onStop() {}

    protected abstract void onRemotePacket(Packet packet);

    protected Logger logger = LoggerFactory.getLogger(getClass());

}
