package org.no.ppp.sos;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public final class FileTileInputStream extends InputStream {

    private int position = 0;

    private ByteBuf buffer = Unpooled.buffer(65536);

    private ReentrantLock streamLock = new ReentrantLock();

    private Condition conditionBufferNotEmpty = streamLock.newCondition();

    private Condition conditionBufferRequired = streamLock.newCondition();

    private Thread watchThread;

    public FileTileInputStream(File file) {
        watchThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    a: while (true) {
                        streamLock.lock();
                        try {
                            conditionBufferRequired.await();
                        } finally {
                            streamLock.unlock();
                        }
                        byte[] bytes = new byte[8192];
                        b: while (true) {
                            try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
                                f.seek(position);
                                int l;
                                while (true) {
                                    l = f.read(bytes);
                                    if (l > 0) {
                                        position += l;
                                        buffer.writeBytes(Unpooled.wrappedBuffer(bytes, 0, l));
                                    }
                                    if (l < bytes.length) {
                                        break;
                                    }
                                }
                                if (buffer.readableBytes() > 0) {
                                    streamLock.lock();
                                    try {
                                        conditionBufferNotEmpty.signal();
                                    } finally {
                                        streamLock.unlock();
                                    }
                                    break b;
                                }
                            }
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                break a;
                            }
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        watchThread.setDaemon(true);
        watchThread.setName("tailWatch-" + watchThread.getId());
        watchThread.start();
    }

    @Override
    public int read() throws IOException {
        streamLock.lock();
        try {
            if (buffer.readableBytes() == 0) {
                conditionBufferRequired.signal();
                if (buffer.readableBytes() == 0) { // check again
                    conditionBufferNotEmpty.await();
                }
            }
            return buffer.readByte() & 0xFF;
        } catch (InterruptedException e) {
            return -1;
        } finally {
            streamLock.unlock();
        }
    }
}