package in.intellicar.layer5.service.namenode.client;

import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;

import java.util.logging.Logger;

public class NameNodeClientInitializer extends ChannelInitializer<SocketChannel>  {
    private Vertx vertx;
    private Logger logger;
    private Message<StorageClsMetaPayload> _event;

    public NameNodeClientInitializer(Vertx lVertx, Message<StorageClsMetaPayload> lEvent, Logger lLogger) {
        this.logger = lLogger;
        this.vertx = lVertx;
        _event = lEvent;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast("client handler", new NameNodeClientHandler(Layer5BeaconParser.getHandler("ClientInit", logger),
                vertx, _event, logger));
    }
}
