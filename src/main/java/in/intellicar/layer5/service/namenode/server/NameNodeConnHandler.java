package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.Layer5Beacon;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.data.Deserialized;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class NameNodeConnHandler extends ChannelInboundHandlerAdapter {
    public static AtomicInteger handlerCount = new AtomicInteger(0);
    public int handlerCountLocal = 0;
    public String scratchDir;
    public Logger logger;
    public Layer5BeaconParser l5parser;
    public Vertx vertx;

    public EventBus eventBus;
    public MessageConsumer<StorageClsMetaPayload> eventConsumer;
    public LinkedBlockingQueue<StorageClsMetaPayload> mailbox;

    public int seqId;
    public ChannelHandlerContext ctx;
    public byte[] localBuffer;
    public int bufwidx;
    public int bufridx;

    public static int MAIL_ADDED = 1;

    public NameNodeConnHandler(String scratchDir, Logger logger, Layer5BeaconParser l5parser, Vertx vertx) {
        super();
        this.handlerCountLocal = handlerCount.incrementAndGet();
        this.scratchDir = scratchDir;
        this.logger = logger;
        this.l5parser = l5parser;
        this.vertx = vertx;

        this.eventBus = vertx.eventBus();
        this.eventConsumer = null;
        this.mailbox = new LinkedBlockingQueue<>();

        this.seqId = 0;
        this.ctx = null;
        this.localBuffer = new byte[16 * 1024];
        this.bufwidx = 0;
        this.bufridx = 0;
    }

    public void cleanUpLocalBuffer(){
        bufridx = 0;
        bufwidx = 0;
    }

    public ArrayList<Layer5Beacon> parseLayer5Beacons(ByteBuf byteBuf) {
        ArrayList<Layer5Beacon> layer5Beacons = new ArrayList<>();
        while (byteBuf.isReadable()) {
            if (localBuffer.length == bufwidx) {
                // Local buffer is totally full, so cleanup the buffer totally
                cleanUpLocalBuffer();
            }
            int bytesToRead = localBuffer.length / 4;
            if (byteBuf.readableBytes() < bytesToRead) {
                bytesToRead = byteBuf.readableBytes();
            }
            if ((localBuffer.length - bufwidx) < bytesToRead) {
                bytesToRead = localBuffer.length - bufwidx;
            }

            byteBuf.readBytes(localBuffer, bufwidx, bytesToRead);
            bufwidx += bytesToRead;
            layer5Beacons.addAll(parseLayer5FromLocalBuf());
        }
        return layer5Beacons;
    }


    public void adjustBuffer() {
        if (bufwidx - bufridx > 0) {
            if (bufridx > 0) {
                System.arraycopy(localBuffer, bufridx, localBuffer, 0, bufwidx - bufridx);
                bufwidx -= bufridx;
                bufridx = 0;
            }
        } else {
            cleanUpLocalBuffer();
        }
    }

    public void skipBytesTillHeader() {
        for (int i = bufridx; i <= (bufwidx - Layer5Beacon.LAYER5HEADER.length ); i++) {
            if (l5parser.isHeaderValid(localBuffer, bufridx, bufwidx, logger)) {
                break;
            }
            bufridx++;
        }
        adjustBuffer();
    }

    public ArrayList<Layer5Beacon> parseLayer5FromLocalBuf() {
        ArrayList<Layer5Beacon> layer5Beacons = new ArrayList<>();

        while (l5parser.isHeaderAvailable(localBuffer, bufridx, bufwidx, logger)) {
            if (!l5parser.isHeaderValid(localBuffer, bufridx, bufwidx, logger)) {
                skipBytesTillHeader();
                continue;
            }
            if (!l5parser.isDataSufficient(localBuffer, bufridx, bufwidx, logger)) {
                break;
            }
            Deserialized<Layer5Beacon> layer5BeaconD = l5parser.deserialize(localBuffer, bufridx, bufwidx, logger);
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

    public void prepareConsumer() {
        if (eventConsumer == null) {
            eventConsumer = eventBus.consumer("/handler/" + handlerCountLocal);
            Handler<Message<StorageClsMetaPayload>> handlerTopicCallback = new Handler<Message<StorageClsMetaPayload>>() {
                @Override
                public void handle(Message<StorageClsMetaPayload> event) {
                    if (ctx != null && !ctx.isRemoved()) {
                        mailbox.add(event.body());
                        ctx.pipeline().fireUserEventTriggered(MAIL_ADDED);
                    }
                }
            };
            eventConsumer.handler(handlerTopicCallback);
        }
    }

    public void handleLayer5Beacons(ArrayList<Layer5Beacon> layer5Beacons) {
        for (Layer5Beacon eachBeacon : layer5Beacons) {
            if (eachBeacon.getBeaconType() != 1) {
                continue;
            }

            StorageClsMetaBeacon storageClsMetaBeacon = (StorageClsMetaBeacon) eachBeacon;
            logger.info("Beacon received::" + storageClsMetaBeacon.toJsonString(logger));
            Handler<AsyncResult<Message<StorageClsMetaPayload>>> replyCallback = new Handler<>() {
                @Override
                public void handle(AsyncResult<Message<StorageClsMetaPayload>> event) {
                    if (ctx != null && !ctx.isRemoved()) {
                        mailbox.add(event.result().body());
                        ctx.pipeline().fireUserEventTriggered(MAIL_ADDED);
                    }
                }
            };

            eventBus.request("/mysqlqueryhandler", storageClsMetaBeacon.getPayload(), replyCallback);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.info("channel read");
        ByteBuf msgBuf = (ByteBuf) msg;
        ArrayList<Layer5Beacon> layer5Beacons = parseLayer5Beacons(msgBuf);
        if (layer5Beacons.size() > 0){
            handleLayer5Beacons(layer5Beacons);
        }
    }

    public void createLA5BeaconAndSend(StorageClsMetaPayload payload){
        try {
            StorageClsMetaBeacon nextBeacon = new StorageClsMetaBeacon(seqId++, payload);
            byte[] nextBeaconRaw = new byte[nextBeacon.getBeaconSize()];
            l5parser.serialize(nextBeaconRaw, 0, nextBeaconRaw.length, nextBeacon, logger);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(nextBeaconRaw)).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("Socket write done");
                }
            });
        }catch (Exception e){
            logger.log(Level.SEVERE, "Exception while creating and sending LA5 Beacon", e);
        }
    }


    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent){
            logger.info("Connection Idle Triggered");
        }else if (evt instanceof Integer){
            Integer action = (Integer) evt;
            if (action == MAIL_ADDED){
                while (mailbox.size() > 0){
                    StorageClsMetaPayload nextPayload = mailbox.poll(1000, TimeUnit.MILLISECONDS);
                    if (nextPayload == null){
                        continue;
                    }
                    createLA5BeaconAndSend(nextPayload);
                }
            }
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        logger.info("channel registered");
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        logger.info("channel unregistered");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        logger.info("Channel active");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        this.ctx = null;
        logger.info("channel inactive");
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        logger.info("channel read complete");
    }


    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        logger.info("channel writability changed");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.info("exception caught");
        logger.log(Level.SEVERE, "Exception caught", cause);
    }
}
