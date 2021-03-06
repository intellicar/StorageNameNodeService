package in.intellicar.layer5.service.namenode;

import in.intellicar.layer5.beacon.storagemetacls.service.common.mysql.MySQLQueryHandler;
import in.intellicar.layer5.beacon.storagemetacls.service.common.netty.NettyTCPServer;
import in.intellicar.layer5.service.namenode.mysql.*;
import in.intellicar.layer5.service.namenode.props.*;
import in.intellicar.layer5.service.namenode.server.*;
import in.intellicar.layer5.utils.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.logging.Logger;

/**
 * @author krishna mohan
 * @version 1.0
 * @project StorageClsAccIDMetaService
 * @date 01/03/21 - 4:38 PM
 */
public class StorageNameNodeService {

    public static void main(String args[]) {
        try {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp %2$s %4$s: %5$s%6$s%n");
        } catch (Exception e) {

        }
        String configFile;
        if (args.length >= 1) {
            configFile = args[0];
        } else {
            System.out.println("Please give the config file path");
            return;
        }

        Logger consoleLogger = Logger.getLogger("generallog");


        // STEP 1: Read receiver properties
        ServerProperties serverProperties = new ServerProperties(configFile, consoleLogger);
        if (!serverProperties.isValid) {
            System.out.println("Receiver properties file is not valid, please check!!!");
            return;
        }
        serverProperties.printDebug(consoleLogger);

        // STEP 2: Create Vertx object for both event bus and schedulers
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(serverProperties.appId, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> event) {
                consoleLogger.info("Vertx Start Status: Is Success" + event.succeeded() +": Event:" + event);
            }
        });

        // STEP 3: Start the MYSQL Query Handler Thread
        MySQLQueryHandler mysqlQueryHandler = new MySQLQueryHandler(vertx, serverProperties.scratchDir, serverProperties.dbMySQLProps,
                new NameNodePayloadHandler(), consoleLogger);
        mysqlQueryHandler.init();
        mysqlQueryHandler.start();

        // STEP 4: Create the Netty Server for receiving Beacon type 1 msgs and replying
        NameNodeChInit channelInit = new NameNodeChInit(serverProperties.scratchDir,
                serverProperties.nettyProps, vertx);
        NettyTCPServer nettyTCPServer = new NettyTCPServer("/tcp/external",
                PathUtils.appendPath(serverProperties.scratchDir, "tcpserver1"), serverProperties.nettyProps, channelInit);
        nettyTCPServer.start();

        while (true) {
            if (PathUtils.fileExists(serverProperties.stopFile, consoleLogger)) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                consoleLogger.info("problem while sleeping");
            }
//            consoleLogger.info("STOPFILE doesn't exist");
        }

        PathUtils.removeFile(serverProperties.stopFile);
        nettyTCPServer.stopReceiver(10000);
        vertx.close();
        consoleLogger.info("Server Shutdown complete");
    }
}
