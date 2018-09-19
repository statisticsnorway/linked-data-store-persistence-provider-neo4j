package no.ssb.lds.core.persistence.neo4j;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.logging.Level;
import java.util.logging.LogManager;

class JavaUtilLoggerBridge {

    private static class Initializer {
        static {
            LogManager.getLogManager().reset();
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
        }

        private static void configure() {
            // method exists to allow access to static initializer
        }
    }

    static final void installJavaUtilLoggerBridgeHandler(Level level) {
        Initializer.configure();
        LogManager.getLogManager().getLogger("").setLevel(level);
    }
}
