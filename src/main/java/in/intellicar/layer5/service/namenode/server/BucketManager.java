package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.SplitBucketReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.IBucketRelatedIdInfoProvider;
import in.intellicar.layer5.beacon.storagemetacls.service.common.mysql.MySQLQueryHandler;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.MySQLProps;
import in.intellicar.layer5.service.namenode.mysql.NameNodePayloadHandler;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class BucketManager extends Thread {

    //private List<Thread> _bucketThreads;
    private Map<BucketInfo, MySQLQueryHandler> _bucketThreadMap;
    private MySQLProps _mySqlProps;
    private Vertx _vertx;
    private LinkedBlockingDeque<Message<StorageClsMetaPayload>> _eventQueue;
    private EventBus _eventBus;
    private static String CONSUMER_ADDRESS_PREFIX = "/mysqlqueryhandler";
    private String _scratchDir;
    private Logger _logger;
    private AtomicBoolean _stop;
    private IBucketEditor _bucketModel;

    public BucketManager(Vertx lVertx, String lScratchDir, MySQLProps lMySQLProps, IBucketEditor lBucketEditor, Logger lLogger)
    {
        _mySqlProps = lMySQLProps;
        _vertx = lVertx;
        _bucketThreadMap = new HashMap<>();
        //_bucketThreads = new ArrayList<>();
        _eventQueue = new LinkedBlockingDeque<>();
        _logger = lLogger;
        _scratchDir = lScratchDir;
        _stop = new AtomicBoolean(false);
        _bucketModel = lBucketEditor;
    }

    public void init()
    {
        Iterator<BucketInfo> it = _mySqlProps.buckets.listIterator();
        while(it.hasNext())
        {
            BucketInfo curBucket = it.next();
            String consumerAddress = CONSUMER_ADDRESS_PREFIX + "/" + curBucket.startBucket.toHex() + "/" + curBucket.endBucket.toHex();
            //String consumerAddress = CONSUMER_ADDRESS_PREFIX;
            MySQLQueryHandler<StorageClsMetaPayload> mysqlQueryHandler = new MySQLQueryHandler(_vertx, _scratchDir, _mySqlProps,
                    new NameNodePayloadHandler(), consumerAddress, _logger);
            mysqlQueryHandler.init();
            mysqlQueryHandler.start();
            _bucketThreadMap.put(curBucket, mysqlQueryHandler);
            _bucketModel.addBucket(curBucket);
        }

        _eventBus = _vertx.eventBus();
        _eventBus.consumer("/mysqlqueryhandler", new Handler<Message<StorageClsMetaPayload>>() {
            @Override
            public void handle(Message<StorageClsMetaPayload> event) {
//                logger.info("Received msg:" + event.body().toJson(logger));
//                handleMySQLQuery(event);
//                eventQueue.add(event);
                _eventQueue.add(event);
            }
        });
        _stop.set(false);
    }

    public void stopService()
    {
        _stop.set(true);
        Collection<MySQLQueryHandler> bucketThreads= _bucketThreadMap.values();
        for(MySQLQueryHandler bThread : bucketThreads)
        {
            bThread.stopService();
        }
        bucketThreads.clear();
    }

    @Override
    public void run() {
        super.run();
        try {
            while (!_stop.get()) {
                Message<StorageClsMetaPayload> event = _eventQueue.poll(1000, TimeUnit.MILLISECONDS);
//                Future<StorageClsMetaPayload> future = Future.future();

                if (event != null && event.body() instanceof SplitBucketReq) {
                    //send it to appropriate bucket handling thread

                    _eventBus.request(getVertexConsumerAddress(event.body()), event);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private String getBucketRelatedIdStringForPayload(StorageClsMetaPayload lRequestPayload)
    {
        String returnValue = "";
       if(lRequestPayload instanceof IBucketRelatedIdInfoProvider)
       {
           SHA256Item idToCheck = ((IBucketRelatedIdInfoProvider)lRequestPayload).getIdReleatedToBucket();
           returnValue = idToCheck.toHex();
       }
       return returnValue;
    }

    private boolean isBucketMatched(String lIdToMatch, BucketInfo lBucket)
    {
        return lIdToMatch.compareToIgnoreCase(lBucket.startBucket.toHex()) > 0
                && lIdToMatch.compareToIgnoreCase(lBucket.endBucket.toHex()) <= 0;
    }

    private String getConsumerAddressForBucket(BucketInfo lBucket)
    {
        return CONSUMER_ADDRESS_PREFIX + "/" + lBucket.startBucket.toHex() + "/" + lBucket.endBucket.toHex();
    }

    private String getVertexConsumerAddress(StorageClsMetaPayload lRequestPayload)
    {
        String idToCheck = getBucketRelatedIdStringForPayload(lRequestPayload);
        String returnValue = "";
        Iterator<BucketInfo> it = _mySqlProps.buckets.listIterator();
        while(it.hasNext())
        {
            BucketInfo curBucket = it.next();
            if(isBucketMatched(idToCheck, curBucket))
            {
                returnValue = getConsumerAddressForBucket(curBucket);
                break;
            }
        }
        return returnValue;
    }

}
