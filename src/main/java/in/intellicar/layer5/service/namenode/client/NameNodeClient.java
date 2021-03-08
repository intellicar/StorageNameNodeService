package in.intellicar.layer5.service.namenode.client;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.vertx.core.Vertx;

import java.util.logging.Logger;

public class NameNodeClient implements Runnable{

    private final String host;
    private final int port;
    private String serverName;
    private Vertx vertx;
    private Logger logger;

    private ChannelFuture channelFuture;
    private Thread clientThread;

    public NameNodeClient(String host, int port, String serverName, Vertx vertx, Logger logger) {
        this.logger = logger;
        this.host = host;
        this.port = port;
        this.vertx = vertx;
        this.channelFuture = null;
        this.clientThread = null;
        this.serverName = serverName;
    }

    public void startClient(){
        clientThread = new Thread(this);
        clientThread.start();
    }

    public void stopClient() throws InterruptedException {
        //_channelFuture.channel().close(); //need to check if this close helps to close the connection or not
        clientThread.join();
    }

    @Override
    public void run() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new NameNodeClientInitializer(serverName, vertx, logger));

            channelFuture = b.connect(host, port).sync();

            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

}
