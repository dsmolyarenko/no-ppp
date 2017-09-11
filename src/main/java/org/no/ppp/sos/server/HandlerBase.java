package org.no.ppp.sos.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.no.ppp.sos.model.Packet;
import org.no.ppp.sos.model.Packet.Type;
import org.no.ppp.sos.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
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

        incoming = new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Pump task started.");
                try {
                    startStreamToQueuePump(is, p -> {
                        switch (p.getType()) {
                            case OPEN:
                                onOpen(p);
                                break;
                            case DATA:
                                onData(p);
                                break;
                            case CLOSE:
                                onClose(p);
                                break;
                            case ERROR:
                                onError(p);
                                break;
                        }
                    });
                } catch (Exception e) {
                    logger.warn("Pump task was interrupted by exception", e);
                    throw new RuntimeException(e);
                }
                logger.info("Pump task finished.");
            }
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
        int length = 0;

        byte[] buffer = new byte[65536 * 2];
        while (true) {
            int b = stream.read(); // assumption that operation is blocking
            if (b == -1) {
                return;
            }
            if (b == '\r' || b == '\n') {
                if (length == 0) {
                    continue;
                }
                Packet packet = Packet.of(buffer, 0, length);
                length = 0;
                if (logger.isInfoEnabled()) {
                    logger.info("<== " + packet);
                }
                consumer.accept(packet);
            } else {
                buffer[length++] = (byte) b;
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

    protected Map<String, ChannelContext> channels = new ConcurrentHashMap<>();

    protected ChannelContext getChannelContext(Packet p) {
        return channels.get(getChannelId(p));
    }

    protected ChannelContext getChannelContext(Channel c) {
        return channels.get(getChannelId(c));
    }

    protected String getChannelId(Packet p) {
        return p.getId();
    }

    protected String getChannelId(Channel channel) {
        return channel.id().asShortText();
    }

    protected ChannelDuplexHandler createChannelHandler() {
        return new ChannelDuplexHandler() {

            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                ChannelContext channelContext = new ChannelContext(getChannelId(ctx.channel()), ctx);
                if (logger.isInfoEnabled()) {
                    logger.info("Engage channel: {}", channelContext.getId());
                }
                channels.put(channelContext.getId(), channelContext);

                onChannelOpen(channelContext);
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                ChannelContext channelContext = getChannelContext(ctx.channel());
                if (logger.isInfoEnabled()) {
                    logger.info("Disarm channel: {}", channelContext.getId());
                }
                channels.remove(channelContext.getId());

                onChannelClose(channelContext);
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ChannelContext channelContext = getChannelContext(ctx.channel());
                outgoingPacketQueue.offer(new Packet(channelContext.getId()).setData(Utils.getBytes((ByteBuf) msg)));
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                cause.printStackTrace();
                ctx.close();
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

        channels.values().forEach(c -> c.getContext().close());

        outgoing.interrupt();

        onStop();
    }

    protected void onStart() throws InterruptedException {

    }

    protected void onStop() throws InterruptedException {

    }

    protected void onChannelOpen(ChannelContext channelContext) {

    }

    protected void onChannelClose(ChannelContext channelContext) {
        if (channelContext.getContext().channel().attr(A_I).get() == null) {
            outgoingPacketQueue.offer(new Packet(channelContext.getId()).setType(Type.CLOSE));
        }
        channelContext.getContext().channel().attr(A_I).set(null);
    }

    protected void onOpen(Packet p) {

    }

    protected void onData(Packet p) {
        ChannelContext channelContext = getChannelContext(p);
        if (channelContext == null) {
            logger.warn("Channel is no longer manageable: {}", p.getId());
            return;
        }
        channelContext.getContext().writeAndFlush(Unpooled.wrappedBuffer(p.getData()));
    }

    protected void onClose(Packet p) {
        ChannelContext channelContext = getChannelContext(p);
        if (channelContext == null) {
            logger.warn("Channel is no longer manageable: {}", p.getId());
            return;
        }
        channelContext.getContext().channel().attr(A_I).set(true);
        channelContext.getContext().channel().close();
    }

    protected void onError(Packet p) {

    }

    protected static class ChannelContext {

        private String id;

        private ChannelHandlerContext context;

        public ChannelContext(String id, ChannelHandlerContext context) {
            super();
            this.id = id;
            this.context = context;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public ChannelHandlerContext getContext() {
            return context;
        }

        public void setContext(ChannelHandlerContext context) {
            this.context = context;
        }
    }

    protected Logger logger = LoggerFactory.getLogger(getClass());

}
