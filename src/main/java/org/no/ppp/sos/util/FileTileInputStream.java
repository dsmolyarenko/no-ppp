package org.no.ppp.sos.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public final class FileTileInputStream extends InputStream {

    private int filePosition = 0;

    private int readPosition = 0;

    private int bytePosition = 0;

    private byte[] bytes = new byte[65536];

    private File file;

    public FileTileInputStream(File file) {
        this.file = file;
    }

    public void fill() {
        bytePosition = 0;
        readPosition = 0;
        try {
            while (true) {
                try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
                    f.seek(filePosition);
                    int length;
                    while (true) {
                        length = f.read(bytes);
                        if (length == -1) {
                            break; // eof
                        }
                        filePosition += length;
                        bytePosition += length;
                        return; // eof or buffer is full
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private boolean first = true;

    @Override
    public int read() throws IOException {
        if (first || readPosition == bytePosition) {
            fill();
            first = false;
        }
        return bytes[readPosition++] & 0xFF;
    }

}