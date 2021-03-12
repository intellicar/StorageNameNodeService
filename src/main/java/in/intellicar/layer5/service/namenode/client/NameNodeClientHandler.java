package in.intellicar.layer5.service.namenode.client;

import in.intellicar.layer5.beacon.Layer5Beacon;
import in.intellicar.layer5.beacon.Layer5BeaconDeserializer;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.AssociatedInstanceIdReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.InstanceIdToBuckReq;
import in.intellicar.layer5.beacon.storagemetacls.service.common.mysql.MySQLQueryHandler;
import in.intellicar.layer5.data.Deserialized;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class NameNodeClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private Logger logger;
    private Layer5BeaconParser l5parser;
    private Vertx vertx;
    private byte[] handlerBuffer;
    private int bufridx;
    private int bufwidx;
    public Message<StorageClsMetaPayload> event = null;
    private static int seqId = 0;
    private ChannelHandlerContext ctx = null;
    StorageClsMetaPayload payload = null;
    private Boolean isActive = false;

    public EventBus eventBus;
    public static int MAIL_ADDED = 1;
    private Object _lock = new Object();
    private String _consumerName;

    public NameNodeClientHandler(Layer5BeaconParser l5parser, Vertx vertx, String lConsumerName, Logger logger){
        this.l5parser = l5parser;
        this.vertx = vertx;
        this.logger = logger;
        _consumerName = lConsumerName;

        this.eventBus = vertx.eventBus();

        Layer5BeaconDeserializer storageMetaClsAPIDeser = new StorageClsMetaBeaconDeser();
        l5parser.registerDeserializer(storageMetaClsAPIDeser.getBeaconType(), storageMetaClsAPIDeser);

        handlerBuffer = new byte[16 * 1024];
        bufridx = 0;
        bufwidx = 0;

        this.eventBus.consumer(_consumerName, (Handler<Message<StorageClsMetaPayload>>) event -> {
            synchronized (_lock) {
                if (this.isActive) {
                    //synchronized (ctx) {//not helpful in dealing with exception
                        this.ctx.pipeline().fireUserEventTriggered(MAIL_ADDED);
                    //}
                }
                this.payload = event.body();
                this.event = event;
            }
        });
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        synchronized (_lock) {
            logger.info("Channel active");
            if (this.payload != null) {
                this.ctx.pipeline().fireUserEventTriggered(MAIL_ADDED);
            }
            this.isActive = true;
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent){
            logger.info("Connection Idle Triggered");
        }else if (evt instanceof Integer){
            Integer action = (Integer) evt;
            if (action == MAIL_ADDED){
                StorageClsMetaBeacon beacon = new StorageClsMetaBeacon(seqId++, this.payload);
                ctx.writeAndFlush(Unpooled.wrappedBuffer(getSerializedByteStream(beacon)))
                        .addListener((ChannelFutureListener) future -> logger.info("Socket write done"));
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        ArrayList<Layer5Beacon> layer5Beacons = parseLayer5Beacons(msg);
        if (layer5Beacons.size() > 0){
            handleLayer5Beacons(layer5Beacons);
        }
        ctx.close().addListener(ChannelFutureListener.CLOSE);
    }

    private byte[] getSerializedByteStream (StorageClsMetaBeacon lBeacon) {
        int beaconSize = lBeacon.getBeaconSize();
        byte[] beaconSerializedBuffer = new byte[beaconSize];
        l5parser.serialize(beaconSerializedBuffer, 0, beaconSize, lBeacon, logger);
        return beaconSerializedBuffer;
    }

    public ArrayList<Layer5Beacon> parseLayer5Beacons(ByteBuf byteBuf) {
        ArrayList<Layer5Beacon> layer5Beacons = new ArrayList<>();
        while (byteBuf.isReadable()) {
            if (handlerBuffer.length == bufwidx) {
                // Local buffer is totally full, so cleanup the buffer totally
                cleanUpLocalBuffer();
            }
            int bytesToRead = handlerBuffer.length / 4;
            if (byteBuf.readableBytes() < bytesToRead) {
                bytesToRead = byteBuf.readableBytes();
            }
            if ((handlerBuffer.length - bufwidx) < bytesToRead) {
                bytesToRead = handlerBuffer.length - bufwidx;
            }

            byteBuf.readBytes(handlerBuffer, bufwidx, bytesToRead);
            bufwidx += bytesToRead;
            layer5Beacons.addAll(parseLayer5FromLocalBuf());
        }
        return layer5Beacons;
    }

    private void adjustBuffer() {
        logger.info("Adjusting buffer: " + bufridx + " , " + bufwidx);
        if (bufwidx - bufridx > 0 && bufridx > 0) {
            System.arraycopy(handlerBuffer, bufridx, handlerBuffer, 0, bufwidx - bufridx);
            bufwidx -= bufridx;
            bufridx = 0;
        }
    }

    private void skipBytesTillHeader() {
        for (int i = bufridx; i < bufwidx; i++) {
//            if (!l5parser.isHeaderAvailable(handlerBuffer, bufridx, bufwidx, logger)){
//                break;
//            }
            if (l5parser.isHeaderValid(handlerBuffer, bufridx, bufwidx, logger)) {
                break;
            }
            bufridx++;
        }
        adjustBuffer();
    }

    public void cleanUpLocalBuffer(){
        bufridx = 0;
        bufwidx = 0;
    }

    public ArrayList<Layer5Beacon> parseLayer5FromLocalBuf() {
        ArrayList<Layer5Beacon> layer5Beacons = new ArrayList<>();

        while (l5parser.isHeaderAvailable(handlerBuffer, bufridx, bufwidx, logger)) {
            if (!l5parser.isHeaderValid(handlerBuffer, bufridx, bufwidx, logger)) {
                skipBytesTillHeader();
                continue;
            }
            if (!l5parser.isDataSufficient(handlerBuffer, bufridx, bufwidx, logger)) {
                break;
            }
            Deserialized<Layer5Beacon> layer5BeaconD = l5parser.deserialize(handlerBuffer, bufridx, bufwidx, logger);
            if (layer5BeaconD.data != null) {
                Layer5Beacon beacon = layer5BeaconD.data;
                layer5Beacons.add(beacon);
                bufridx = layer5BeaconD.curridx;
                adjustBuffer();
            } else {
                bufridx++;
            }
        }
        return layer5Beacons;
    }

    public void handleLayer5Beacons(ArrayList<Layer5Beacon> layer5Beacons) {

        for (Layer5Beacon eachBeacon : layer5Beacons) {
            if (eachBeacon.getBeaconType() != 1) {
                continue;
            }
            StorageClsMetaBeacon storageClsMetaBeacon = (StorageClsMetaBeacon) eachBeacon;
            logger.info("Beacon received::" + storageClsMetaBeacon.toJsonString(logger));
            if (event!= null){
                event.reply(storageClsMetaBeacon.payload);
            }
        }
    }

}