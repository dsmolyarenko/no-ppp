package org.no.ppp.sos;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.core.lookup.MainMapLookup;
import org.no.ppp.sos.server.HandlerClient;
import org.no.ppp.sos.server.HandlerServer;

public class Bootstrap {

    public static void main(String[] arguments) throws IOException, InterruptedException {

        Options options = new Options()
            .addOption(option("h", "help"  , false))
            .addOption(option("s", "server", false))
            .addOption(option("c", "client", false))
            .addOption(option("v", "verbose", false))
            .addOption(option("i", null, true))
            .addOption(option("o", null, true))
            .addOption(option("H", "host", true))
            .addOption(option("P", "port", true))
        ;

        boolean isServer;
        boolean isClient;

        InputStream is;
        OutputStream os;

        String host;
        Integer port;

        try {
            CommandLine cl = new DefaultParser().parse(options, arguments);
            if (cl.hasOption("h")) {
                help(options, null);
            }
            isServer = cl.hasOption("s");
            isClient = cl.hasOption("c");

            if (!(isServer ^ isClient)) {
                throw new ParseException("Either -s or -c should be used");
            }

            //
            // read logging properties
            //

            String level = "warn";
            if (cl.hasOption("v")) {
                level = "info";
            }
            if (cl.hasOption("q")) {
                level = "fatal";
            }
            MainMapLookup.setMainArguments(level);

            //
            // read file properties
            //

            String iPath = cl.getOptionValue("i", "-");
            if (iPath.equals("-")) {
                is = System.in;
            } else {
                is = new FileInputStream(prepareFile(iPath));
            }

            String oPath = cl.getOptionValue("o", "-");
            if (oPath.equals("-")) {
                os = System.out;
            } else {
                os = new FileOutputStream(prepareFile(oPath));
            }

            //
            // socket properties
            //

            String sHost = cl.getOptionValue("H", "localhost");
            String sPort = cl.getOptionValue("P", "3128");

            host = sHost;
            port = Integer.valueOf(sPort);

        } catch (ParseException e) {
            help(options, e);
            throw new Error();
        }

        if (isServer) {
            HandlerServer server = new HandlerServer(is, os, host, port);
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    server.stop();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }));
        }

        if (isClient) {
            HandlerClient client = new HandlerClient(is, os, host, port);
            client.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        client.stop();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
        }
    }

    private static File prepareFile(String iPath) throws IOException, FileNotFoundException {
        File file = new File(iPath);
        if (!file.exists()) {
            file.createNewFile();
        }
        if (file.length() > 0) {
            new PrintWriter(file).close();
        }
        return file;
    }

    private static Option option(String name, String full, boolean argument) {
        return new Option(name, full, argument, null);
    }

    private static void help(Options options, ParseException e) {
        new HelpFormatter().printHelp(
            "program [-i file] [-o file] [-H host] [-P port]",
            e != null ? e.getMessage() : null,
            options,
            null
        );
        System.exit(128);
    }

}
