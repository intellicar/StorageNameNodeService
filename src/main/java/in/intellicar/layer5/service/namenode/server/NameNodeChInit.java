package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.Layer5BeaconDeserializer;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.NettyProps;
import in.intellicar.layer5.beacon.storagemetacls.service.common.vertx.StorageClsMtPayloadCodec;
import in.intellicar.layer5.utils.LogUtils;
import in.intellicar.layer5.utils.PathUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.vertx.core.Vertx;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class NameNodeChInit extends ChannelInitializer<SocketChannel> {
    public String scratchDir;
    public NettyProps nettyProps;
    public Vertx vertx;

    public Logger logger;
    public Layer5BeaconParser l5parser;

    public NameNodeChInit(String scratchDir, NettyProps nettyProps, Vertx vertx) {
        this.scratchDir = PathUtils.appendPath(scratchDir, "channelh");
        this.nettyProps = nettyProps;
        this.vertx = vertx;

        this.logger = LogUtils.createLogger(this.scratchDir, NameNodeChInit.class.getName(), "handler");
        l5parser = Layer5BeaconParser.getHandler(NameNodeChInit.class.getName(), this.logger);

        Layer5BeaconDeserializer storageMetaClsAPIDeser = new StorageClsMetaBeaconDeser();
        l5parser.registerDeserializer(storageMetaClsAPIDeser.getBeaconType(), storageMetaClsAPIDeser);
        RegisterVertxCodecsForAllPayloads();
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().
                addLast(new IdleStateHandler((long)nettyProps.heartBeatInterval,0L,0L,
                        TimeUnit.MILLISECONDS)).
                addLast(new NameNodeConnHandler(this.scratchDir, this.logger, l5parser, vertx));
    }

    /**
     * Registers all the codecs for classes those extend StorageClsMetaPayload, to eventbus of vertx.
     * We tried using register() and registerDefaultCodec() on StorageClsMetaPayload class, but still vertx is
     * complaining about individual payload classes. So we are registering codec for every payload here.
     */
    private void RegisterVertxCodecsForAllPayloads()
    {
        for (PayloadTypes eachType : EnumSet.allOf(PayloadTypes.class))
        {
            Class payloadClass = eachType.getAssociatedPayloadClass();
            if(payloadClass != null)
                vertx.eventBus().registerDefaultCodec(payloadClass,
                        new StorageClsMtPayloadCodec<>(payloadClass, logger));
            else
                logger.log(Level.SEVERE, "trying to register codec for payload " + eachType.getSubType() + " , missed class association with the enum ?");
        }
    }
}
