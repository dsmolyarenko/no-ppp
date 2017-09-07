package org.no.ppp.sos.server;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.no.ppp.sos.model.Packet;
import org.no.ppp.sos.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
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

        incoming = new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Pump task started.");
                try {
                    startStreamToQueuePump(is, new Consumer<Packet>() {
                        @Override
                        public void accept(Packet p) {
                            onRemotePacket(p);
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

        byte[] buffer = new byte[65536];
        while (true) {
            int b = stream.read(); // assumption that operation is blocking
            if (b == '\r' || b == '\n') {
                if (length == 0) {
                    continue;
                }
                Packet packet = Packet.of(buffer, 0, length); length = 0;
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

    /**
     * Channel local dictionary.
     */
    private Map<ChannelId, String> ids = new ConcurrentHashMap<>();

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
                outgoingPacketQueue.offer(new Packet(id).setData(Utils.getBytes((ByteBuf) msg)));

                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
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

    public static void main(String[] args) throws FileNotFoundException, IOException {
        try (ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream("test1.bin"))) {
            stream.writeObject(new Packet("1x"));
        }
        try (ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream("test2.bin"))) {
            stream.writeObject(new Packet("2xx"));
        }
    }

}
