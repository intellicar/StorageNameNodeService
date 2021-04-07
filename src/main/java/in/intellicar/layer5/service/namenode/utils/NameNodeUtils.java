package in.intellicar.layer5.service.namenode.utils;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.AssociatedInstanceIdReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.AssociatedInstanceIdRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.*;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterRsp;
import in.intellicar.layer5.service.namenode.client.NameNodeClient;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import in.intellicar.layer5.utils.sha.SHA256Utils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class NameNodeUtils {
    private static AtomicInteger _consumerAddressSuffix = new AtomicInteger(0);
    private static final String CONSUMER_NAME_PREFIX = "/clientreqhandler";
    private static final String ZOOKEEPER_IP = "localhost";
    private static final int ZOOKEEPER_PORT = 10107;
    // Utility Functions
    private static String getIpFromBytes(byte[] lIpBytes) {
        String ipString = "";
        if(lIpBytes.length == 4)
            ipString = Byte.toUnsignedInt(lIpBytes[0]) + "." + Byte.toUnsignedInt(lIpBytes[1]) + "." +
                    Byte.toUnsignedInt(lIpBytes[2]) + "." + Byte.toUnsignedInt(lIpBytes[3]);
        return ipString;
    }

    private static Thread setUpAndStartClient(String lIpString, int lPort, Vertx lVertx, String lConsumerName, Logger lLogger) {
        NameNodeClient client = new NameNodeClient(lIpString, lPort, lVertx, null, lLogger);
        Thread clientThread = new Thread(client);
        EventBus eventBus = lVertx.eventBus();
        eventBus.consumer(lConsumerName, (Handler<Message<StorageClsMetaPayload>>) event -> {
            client.setEvent(event);
            clientThread.start();
        });
        return clientThread;
    }
    /*
    TODO: Client End
    1. Get InstanceId corresponding to accountName
    2. Send AccIdRegisterReq to InstanceId
    **/
    public static AssociatedInstanceIdRsp getInstanceID(Vertx lVertx, SHA256Item lIdToBeMatched, Logger logger) {
        AssociatedInstanceIdReq req = new AssociatedInstanceIdReq(lIdToBeMatched);
        //StorageClsMetaBeacon beacon = new StorageClsMetaBeacon(seqID, req);
        String consumerName = CONSUMER_NAME_PREFIX + _consumerAddressSuffix.getAndIncrement();
        AssociatedInstanceIdRsp returnValue = null;

        Thread clientThread = setUpAndStartClient(ZOOKEEPER_IP, ZOOKEEPER_PORT, lVertx, consumerName, logger);

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request(consumerName, req);

        doWaitOnFuture(future);
        if (future.succeeded()) {
            returnValue = (AssociatedInstanceIdRsp) future.result().body();
        } else {
            Throwable cause = future.cause();
            if (cause != null)
                logger.info("getInstanceId failed with error: " + cause.getMessage());
            else
                logger.info("getInstanceID failed");
        }
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    public static void getInstanceID(Vertx lVertx, SHA256Item lIdToBeMatched, Handler<AsyncResult<Message<StorageClsMetaPayload>>> lCallback, Logger logger) {
        AssociatedInstanceIdReq req = new AssociatedInstanceIdReq(lIdToBeMatched);
        //StorageClsMetaBeacon beacon = new StorageClsMetaBeacon(seqID, req);
        String consumerName = CONSUMER_NAME_PREFIX + _consumerAddressSuffix.getAndIncrement();

        Thread clientThread = setUpAndStartClient(ZOOKEEPER_IP, ZOOKEEPER_PORT, lVertx, consumerName, logger);

        EventBus eventBus = lVertx.eventBus();
        eventBus.request(consumerName, req, lCallback);
        //TODO:: not making sure, death of thread; join need to be takencare
    }

    private static <T> void doWaitOnFuture(Future<T> lFuture) {
        while (true) {
            synchronized (lFuture) {

                if(lFuture.isComplete()) {
                    return;
                }

                try {
                    lFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * This method will be used where ever we need to generate Ids. Individual generate methods will be replaced by this method
     * @param lIdsAndStringsMakingAbsPath
     * @param salt
     * @return
     */
    public static SHA256Item generateIdForPath(List<byte[]>lIdsAndStringsMakingAbsPath, byte[] salt) {
        int totalLength = 0;
        for (byte[] idBytes: lIdsAndStringsMakingAbsPath)
        {
            totalLength += idBytes.length + 1;
        }
        byte[] saltyName = new byte[totalLength + salt.length];
        int curIdx = 0;
        for (byte[] idBytes: lIdsAndStringsMakingAbsPath)
        {
            System.arraycopy(idBytes, 0, saltyName, curIdx, idBytes.length);
            saltyName[curIdx + idBytes.length] = '/';
            curIdx += idBytes.length + 1;
        }
        System.arraycopy(salt, 0, saltyName, curIdx, salt.length);

        try {
            return SHA256Utils.getSHA256(saltyName);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SHA256Item checkAccountID(String accountName, MySQLPool vertxMySQLClient, Logger logger) {

        SHA256Item returnValue = null;
        //TODO update db.table_name
        String sql = "SELECT account_id from accounts.account_info where account_name = '" + accountName + "'";
        Future<RowSet<Row>> future = vertxMySQLClient
                .query(sql)
                .execute();

        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                String hexID = rows.iterator().next().getString("account_id"); // TODO update column name
                returnValue = new SHA256Item(LittleEndianUtils.hexStringToByteArray(hexID));
            } else {
                logger.info("succeeded, but rows aren't correct");
            }
        }
        else {
            logger.info("checkAccountID failed with an error: " + future.cause().getMessage());
        }
        return returnValue;
    }

    public static SHA256Item generateAccountId(byte[] nameBytes, byte[] saltBytes) {

        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(nameBytes);
        return generateIdForPath(pathArray, saltBytes);
    }

    public static void sendRegisterAccountIdRequest(AssociatedInstanceIdRsp lInstanceIdRsp, AccIdGenerateRsp lAccIdGenRsp, Vertx lVertx, Handler<AsyncResult<Message<StorageClsMetaPayload>>> lCallback, Logger lLogger) {
        AccIdRegisterReq accIdRegReq = new AccIdRegisterReq(lAccIdGenRsp.accountID, lAccIdGenRsp.accIdGenerateReq.accNameUtf8Bytes, ((IActAsClient)lAccIdGenRsp).getSalt().getBytes(StandardCharsets.UTF_8));
        String ipString = getIpFromBytes(lInstanceIdRsp.ip);
        String consumerName = CONSUMER_NAME_PREFIX + _consumerAddressSuffix.getAndIncrement();

        Thread clientThread = setUpAndStartClient(ipString, lInstanceIdRsp.port, lVertx, consumerName, lLogger);

        EventBus eventBus = lVertx.eventBus();
        eventBus.request(consumerName, accIdRegReq, lCallback);
        //TODO:: not making sure, death of thread; join need to be takencare
    }

    public static AccIdRegisterRsp sendRegisterAccountIdRequest(AssociatedInstanceIdRsp lInstanceIdRsp, SHA256Item lAccId, String lAccName, String lSalt, Vertx lVertx, Logger lLogger) {
        AccIdRegisterReq accIdRegReq = new AccIdRegisterReq(lAccId, lAccName.getBytes(StandardCharsets.UTF_8), lSalt.getBytes(StandardCharsets.UTF_8));
        String ipString = getIpFromBytes(lInstanceIdRsp.ip);
        String consumerName = CONSUMER_NAME_PREFIX + _consumerAddressSuffix.getAndIncrement();
        AccIdRegisterRsp returnValue = null;

        Thread clientThread = setUpAndStartClient(ipString, lInstanceIdRsp.port, lVertx, consumerName, lLogger);

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request(consumerName, accIdRegReq);

        doWaitOnFuture(future);
        if (future.succeeded()) {
            returnValue = (AccIdRegisterRsp) future.result().body();
        } else {
            Throwable cause = future.cause();
            if(cause != null)
                lLogger.info("getInstanceId failed with error: " + cause.getMessage());
            else
                lLogger.info("getInstanceID failed");
        }
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return returnValue;

    }

    public static void updateAckOfAccName(AccIdRegisterRsp lRsp, MySQLPool lVertxMySQLClient, Logger logger) {
        String accName = new String(lRsp.accIdRegisterReq.accountNameUtf8Bytes, StandardCharsets.UTF_8);
        String sql = "Update accounts.account_info set ack = '"+ lRsp.ackFlag +"' where account_name = '" + accName + "'";
        Future<RowSet<Row>> updateFuture = lVertxMySQLClient
                .query(sql)
                .execute();

      doWaitOnFuture(updateFuture);
        if (updateFuture.succeeded()) {
            return ;//TODO:: return type need to be added to handle errors
        } else {
            return ;
        }
    }

    public static StorageClsMetaPayload getAccountID(AccIdGenerateReq lReq, MySQLPool vertxMySQLClient, Logger logger) {
        String accountName = new String(lReq.accNameUtf8Bytes, StandardCharsets.UTF_8);
        StorageClsMetaPayload returnValue = null;
        SHA256Item checkedAccountId = checkAccountID(accountName, vertxMySQLClient, logger);
        if (checkedAccountId!= null) {
            returnValue = new AccIdGenerateRsp(lReq, checkedAccountId);
        } else {
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item accountIDSHA = generateAccountId(lReq.accNameUtf8Bytes, salt.getBytes());
//        String salt = Long.toHexString(System.nanoTime());
//        SHA256Item accountIDSHA = null;
//        AssociatedInstanceIdRsp instanceIdRsp = null;
//
//        // TODO: Remove this later
//        if (checkedAccountIDFuture.succeeded()) {
//            accountIDSHA =  checkedAccountIDFuture.result();
//            instanceIdRsp = getInstanceID(lVertx, accountIDSHA, logger);
//            AccIdRegisterRsp accIdRegisterRsp = sendRegisterAccountIdRequest(instanceIdRsp, accountIDSHA, accountName, salt, lVertx, logger);
//            updateAckOfAccName(accIdRegisterRsp, vertxMySQLClient, logger);
//            return Future.succeededFuture(accountIDSHA);
//        }
//
//        else {
//            accountIDSHA = generateAccountId(req.accNameUtf8Bytes, salt.getBytes());
            //TODO: Update db.table_name
            SHA256Item accountNameSHA = null;
            try {
                accountNameSHA = SHA256Utils.getSHA256(lReq.accNameUtf8Bytes);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Future<RowSet<Row>> insertFuture = vertxMySQLClient.preparedQuery("INSERT INTO accounts.account_info (account_name, account_name_sha, salt, account_id, ack) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(accountName, accountNameSHA.toHex(),salt, accountIDSHA.toHex(), 0));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                //instanceIdRsp = getInstanceID(lVertx, accountIDSHA, logger);
                //AccIdRegisterRsp accIdRegisterRsp = sendRegisterAccountIdRequest(instanceIdRsp, accountIDSHA, accountName, salt, lVertx, logger);
                //updateAckOfAccName(accIdRegisterRsp, vertxMySQLClient, logger);
                AccIdGenerateRsp accIdGenerateRsp = new AccIdGenerateRsp(lReq, accountIDSHA);
                accIdGenerateRsp.isToBeRegistered(true);
                accIdGenerateRsp.setSalt(salt);
                returnValue = accIdGenerateRsp;
            }
            else {
                Throwable cause = insertFuture.cause();
                logger.info("getAccountID failed with an error: " + cause.getMessage());
                returnValue = new StorageClsMetaErrorRsp(cause.getMessage(), lReq);
            }
        }
        return returnValue;
    }

    public static StorageClsMetaPayload registerAccountID(AccIdRegisterReq lReq,  MySQLPool lVertxMySQLClient, Logger lLogger){
        String accountName = new String(lReq.accountNameUtf8Bytes, StandardCharsets.UTF_8);
        String saltString = new String(lReq.saltBytes, StandardCharsets.UTF_8);
        StorageClsMetaPayload returnValue = null;
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT IGNORE INTO accounts.account_id_info (account_id, account_name, salt) values (?, ?, ?)")
                .execute(Tuple.of(lReq.accountId.toHex(), accountName, saltString));

        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            AccIdRegisterRsp accIdRegisterRsp = new AccIdRegisterRsp(lReq, 1);
            returnValue = accIdRegisterRsp;
        } else {
            Throwable cause = insertFuture.cause();
            lLogger.info("registerAccountID failed with an error: " + cause.getMessage());
            returnValue = new StorageClsMetaErrorRsp(cause.getMessage(), lReq);
        }
        return returnValue;
    }

    public static SHA256Item checkNamespaceId(NsIdGenerateReq req, MySQLPool vertxMySQLClient, Logger logger) {
        String namespaceName = new String(req.namespaceBytes, StandardCharsets.UTF_8);
        SHA256Item returnValue = null;
        String sql = "SELECT namespace_id from accounts.namespaceinfo where namespace_name = '" + namespaceName + "' and account_id = '" + req.accountID.toHex() + "'";
        Future<RowSet<Row>> future = vertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                String idHexString = rows.iterator().next().getString("namespace_id");
                SHA256Item namespaceID = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                returnValue = namespaceID;
            } else {
                logger.info("checkNamespaceId succeeded, but rows aren't correct");
            }
        }
        else{
            Throwable cause = future.cause();
            logger.info("checkNamespaceId failed with an error: " + cause.getMessage());
        }
        return returnValue;
    }

    public static SHA256Item generateNamespaceId(byte[] accountIDbytes, byte[] nameBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(accountIDbytes);
        pathArray.add(nameBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static void sendRegisterNsIdRequest(AssociatedInstanceIdRsp lInstanceIdRsp, NsIdGenerateRsp lNsIdGenRsp, Vertx lVertx, Handler<AsyncResult<Message<StorageClsMetaPayload>>> lCallback, Logger lLogger) {
        NsIdRegisterReq nsIdRegReq = new NsIdRegisterReq(lNsIdGenRsp.namespaceID, lNsIdGenRsp.nsIdGenerateReq.accountID, lNsIdGenRsp.nsIdGenerateReq.namespaceBytes, ((IActAsClient)lNsIdGenRsp).getSalt().getBytes(StandardCharsets.UTF_8));
        String ipString = getIpFromBytes(lInstanceIdRsp.ip);
        String consumerName = CONSUMER_NAME_PREFIX + _consumerAddressSuffix.getAndIncrement();

        Thread clientThread = setUpAndStartClient(ipString, lInstanceIdRsp.port, lVertx, consumerName, lLogger);

        EventBus eventBus = lVertx.eventBus();
        eventBus.request(consumerName, nsIdRegReq, lCallback);
        //TODO:: not making sure, death of thread; join need to be takencare
    }

    public static NsIdRegisterRsp sendRegisterNsIdRequest(AssociatedInstanceIdRsp lInstanceIdRsp, SHA256Item lNsId, SHA256Item lAccId, String lNsName, String lSalt, Vertx lVertx, Logger lLogger) {
        NsIdRegisterReq nsIdRegReq = new NsIdRegisterReq(lNsId, lAccId, lNsName.getBytes(StandardCharsets.UTF_8), lSalt.getBytes(StandardCharsets.UTF_8));
        String ipString = getIpFromBytes(lInstanceIdRsp.ip);
        String consumerName = CONSUMER_NAME_PREFIX + _consumerAddressSuffix.getAndIncrement();
        NsIdRegisterRsp returnValue = null;

        Thread clientThread = setUpAndStartClient(ipString, lInstanceIdRsp.port, lVertx, consumerName, lLogger);

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request(consumerName, nsIdRegReq);

        doWaitOnFuture(future);
        if (future.succeeded()) {
            returnValue = (NsIdRegisterRsp) future.result().body();
        } else {
            Throwable cause = future.cause();
            if(cause != null)
                lLogger.info("getInstanceId failed with error: " + cause.getMessage());
            else
                lLogger.info("getInstanceID failed");
        }
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    public static void updateAckOfNsName(NsIdRegisterRsp lRsp, MySQLPool lVertxMySQLClient, Logger logger) {
        String nsName = new String(lRsp.nsIdRegisterReq.nsNameUtf8Bytes, StandardCharsets.UTF_8);
        String sql = "Update accounts.namespace_info set ack = '"+ lRsp.ackFlag +"' where namespace_name = '" + nsName +
                "' and account_id = '" + lRsp.nsIdRegisterReq.accountId.toHex() + "'";
        Future<RowSet<Row>> updateFuture = lVertxMySQLClient
                .query(sql)
                .execute();

        doWaitOnFuture(updateFuture);
        if (updateFuture.succeeded()) {
            return ;//TODO:: return type need to be added to handle errors
        } else {
            return ;
        }
    }

    public static StorageClsMetaPayload getNamespaceId(NsIdGenerateReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        SHA256Item existingNsId = checkNamespaceId(lReq, lVertxMySQLClient, lLogger);
        StorageClsMetaPayload returnValue = null;
        if (existingNsId != null) {
            returnValue = new NsIdGenerateRsp(lReq, existingNsId);
        } else {
            String namespaceName = new String(lReq.namespaceBytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item namespaceID = generateNamespaceId(lReq.accountID.hashdata, lReq.namespaceBytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT IGNORE INTO accounts.namespace_info (account_id, namespace_name, salt, namespace_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.accountID.toHex(), namespaceName, salt, namespaceID.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                //AssociatedInstanceIdRsp instanceIdRsp = getInstanceID(lVertx, namespaceID, lLogger);
                //NsIdRegisterRsp nsIdRegisterRsp = sendRegisterNsIdRequest(instanceIdRsp, namespaceID, lReq.accountID, namespaceName, salt, lVertx, lLogger);
                //updateAckOfNsName(nsIdRegisterRsp, lVertxMySQLClient, lLogger);
                NsIdGenerateRsp nsIdGenerateRsp = new NsIdGenerateRsp(lReq, namespaceID);
                nsIdGenerateRsp.isToBeRegistered(true);
                nsIdGenerateRsp.setSalt(salt);
                returnValue = nsIdGenerateRsp;
            } else {
                Throwable cause = insertFuture.cause();
                lLogger.info("getNamespaceId failed with an error: " + cause.getMessage());
                returnValue = new StorageClsMetaErrorRsp(cause.getMessage(), lReq);
            }
        }
        return  returnValue;
    }

    public static StorageClsMetaPayload registerNsId(NsIdRegisterReq lReq,  MySQLPool lVertxMySQLClient, Logger lLogger){
        String nsName = new String(lReq.nsNameUtf8Bytes, StandardCharsets.UTF_8);
        String saltString = new String(lReq.saltBytes, StandardCharsets.UTF_8);
        StorageClsMetaPayload returnValue = null;
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT IGNORE INTO accounts.namespace_id_info (namespace_id, account_id, namespace_name, salt) values (?, ?, ?, ?)")
                .execute(Tuple.of(lReq.nsId.toHex(), lReq.accountId.toHex(), nsName, saltString));

        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            NsIdRegisterRsp nsIdRegisterRsp = new NsIdRegisterRsp(lReq, 1);
            returnValue = nsIdRegisterRsp;
        } else {
            Throwable cause = insertFuture.cause();
            lLogger.info("registerNsId failed with an error: " + cause.getMessage());
            returnValue = new StorageClsMetaErrorRsp(cause.getMessage(), lReq);
        }
        return returnValue;
    }

    public static SHA256Item checkDirId(DirIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
        SHA256Item returnValue = null;
        String sql = "SELECT dir_id from accounts.dir_info where namespace_id = '" + lReq.nsId.toHex() +
                "' and parent_dir_id = '" + (lReq.hasParentDir == 1 ? lReq.parentDirId.toHex(): "") +
                "' and dir_name = '" + dirName + "'";
        Future<RowSet<Row>> future = lVertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                String idHexString = rows.iterator().next().getString("dir_id");
                SHA256Item dirId = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                returnValue = dirId;
            } else {
                lLogger.info("checkDirId succeeded, but rows aren't correct");
            }
        }
        else{
            Throwable cause = future.cause();
            lLogger.info("checkDirId failed with an error: " + cause.getMessage());
        }
        return returnValue;
    }

    public static SHA256Item generateDirId(byte[] lNsIdBytes, boolean lHasParentDir, byte[] lParentDirid, byte[] lDirNameBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(lNsIdBytes);
        if(lHasParentDir)
            pathArray.add(lParentDirid);
        pathArray.add(lDirNameBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static StorageClsMetaPayload getDirId(DirIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        StorageClsMetaPayload returnValue = null;
        SHA256Item existingDirId = checkDirId(lReq, lVertxMySQLClient, lLogger);
        if (existingDirId != null) {
            returnValue = new DirIdGenerateAndRegisterRsp(lReq, existingDirId);
        } else {
            String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item dirId = generateDirId(lReq.nsId.hashdata, lReq.hasParentDir == 1,
                    lReq.hasParentDir == 1? lReq.parentDirId.hashdata: null, lReq.dirNameUtf8Bytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT IGNORE INTO accounts.dir_info (namespace_id, parent_dir_id, dir_name, salt, dir_id) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.nsId.toHex(), (lReq.hasParentDir == 1?lReq.parentDirId.toHex():""), dirName, salt, dirId.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                registerDirId(dirId, lReq, salt, lVertxMySQLClient);
                returnValue = new DirIdGenerateAndRegisterRsp(lReq, dirId);
            } else {
                Throwable cause = insertFuture.cause();
                lLogger.info("getDirId failed with an error: " + cause.getMessage());
                returnValue = new StorageClsMetaErrorRsp(cause.getMessage(), lReq);
            }
        }
        return returnValue;
    }

    public static void registerDirId(SHA256Item lDirId, DirIdGenerateAndRegisterReq lReq, String lSalt, MySQLPool lVertxMySQLClient) {
        String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.dir_id_info (dir_id, namespace_id, parent_dir_id, dir_name, salt) values (?, ?, ?, ?, ?)")
                .execute(Tuple.of(lDirId.toHex(), lReq.nsId.toHex(), (lReq.hasParentDir == 1?lReq.parentDirId.toHex():""), dirName, lSalt));
        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return ;
        } else {
            return ;
        }
    }

    public static SHA256Item checkFileId(FileIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
        SHA256Item returnValue = null;
        String sql = "SELECT file_id from accounts.file_info where namespace_id = '" + lReq.nsId.toHex() +
                "' and parent_dir_id = '" + lReq.parentDirId.toHex() +
                "' and file_name = '" + fileName + "'";
        Future<RowSet<Row>> future = lVertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                String idHexString = rows.iterator().next().getString("file_id");
                SHA256Item fileId = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                returnValue = fileId;
            } else {
                lLogger.info("checkFileId succeeded, but rows aren't correct");
            }
        }
        else{
            Throwable cause = future.cause();
            lLogger.info("checkFileId failed with an error: " + cause.getMessage());
        }
        return returnValue;
    }

    public static SHA256Item generateFileId(byte[] lNsIdBytes, byte[] lParentDirid, byte[] lFileNameBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(lNsIdBytes);
        pathArray.add(lParentDirid);
        pathArray.add(lFileNameBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static StorageClsMetaPayload getFileId(FileIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        StorageClsMetaPayload returnValue = null;
        SHA256Item existingFileId = checkFileId(lReq, lVertxMySQLClient, lLogger);
        if (existingFileId != null) {
            returnValue = new FileIdGenerateAndRegisterRsp(lReq, existingFileId);
        } else {
            String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item fileId = generateFileId(lReq.nsId.hashdata,
                    lReq.parentDirId.hashdata, lReq.fileNameUtf8Bytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT IGNORE INTO accounts.file_info (namespace_id, parent_dir_id, file_name, salt, file_id) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.nsId.toHex(), lReq.parentDirId.toHex(), fileName, salt, fileId.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                registerFileId(fileId, lReq, salt, lVertxMySQLClient); // register file_id
                addFileVersion(fileId, lVertxMySQLClient);//adding file_version
                returnValue = new FileIdGenerateAndRegisterRsp(lReq, fileId);
            } else {
                Throwable cause = insertFuture.cause();
                lLogger.info("getFileId failed with an error: " + cause.getMessage());
                returnValue = new StorageClsMetaErrorRsp(cause.getMessage(), lReq);
            }
        }
        return returnValue;
    }

    public static void registerFileId(SHA256Item lFileId, FileIdGenerateAndRegisterReq lReq, String lSalt, MySQLPool lVertxMySQLClient) {
        String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_id_info (file_id, namespace_id, parent_dir_id, file_name, salt) values (?, ?, ?, ?, ?)")
                .execute(Tuple.of(lFileId.toHex(), lReq.nsId.toHex(), lReq.parentDirId.toHex(), fileName, lSalt));
        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return ;
        } else {
            return ;
        }
    }
    public static void addFileVersion(SHA256Item lFileId, MySQLPool lVertxMySQLClient) {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_info (file_id, file_version) values (?, ?)")
                .execute(Tuple.of(lFileId.toHex(), 0));

        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return;//TODO:: return type need to be added to handle errors
        } else {
            return ;
        }
    }

    public static Integer getAndUpdateFileVersionNo(FileVersionIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        int returnValue = -1;
        String sql = "SELECT file_version from accounts.file_version_info where file_id = '" + lReq.fileId.toHex() + "'";
        Future<RowSet<Row>> future = lVertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                int fileVersion = rows.iterator().next().getInteger("file_version");
                updateFileVersion(lReq.fileId, lVertxMySQLClient);
                returnValue = fileVersion;
            }
            else
            {
                lLogger.info("getAndUpdateFileVersionNo succeeded, but rows aren't correct");
            }
        } else {
            Throwable cause = future.cause();
            lLogger.info("getAndUpdateFileVersionNo failed with an error: " + cause.getMessage());
        }
        return returnValue;
    }

    public static void updateFileVersion(SHA256Item lFileId, MySQLPool lVertxMySQLClient) {
        String sql = "Update accounts.file_version_info set file_version = file_version + 1 where file_id = '" + lFileId.toHex() + "'";
        Future<RowSet<Row>> updateFuture = lVertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(updateFuture);
        if (updateFuture.succeeded()) {
            return ;//TODO:: return type need to be added to handle errors
        } else {
            return ;
        }
    }


    public static SHA256Item generateFileVersionId(byte[] lFileIdBytes, byte[] lVersionNoBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(lFileIdBytes);
        pathArray.add(lVersionNoBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static StorageClsMetaPayload getFileVersionId(FileVersionIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        StorageClsMetaPayload returnValue = null;
        Integer fileVersion = getAndUpdateFileVersionNo(lReq, lVertxMySQLClient, lLogger);
        if (fileVersion >= 0) {
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item fileVersionId = generateFileVersionId(lReq.fileId.hashdata,
                    BigInteger.valueOf(fileVersion).toByteArray(), salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT IGNORE INTO accounts.file_version_id_info (file_id, file_version, salt, file_version_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.fileId.toHex(), fileVersion, salt, fileVersionId.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                registerFileVersionId(fileVersionId, lReq, fileVersion, salt, lVertxMySQLClient);
                returnValue = new FileVersionIdGenerateAndRegisterRsp(lReq, fileVersionId);
            } else {
                Throwable cause = insertFuture.cause();
                lLogger.info("getFileVersionId failed with an error: " + cause.getMessage());
            }
        } else {
            lLogger.info("getAndUpdateFileVersionNo failed with an error");
        }
        return returnValue;
    }

    public static void registerFileVersionId(SHA256Item lFileVersionId, FileVersionIdGenerateAndRegisterReq lReq, int lFileVersion, String lSalt, MySQLPool lVertxMySQLClient) {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_reg_info (file_version_id, file_id, file_version, salt) values (?, ?, ?, ?)")
                .execute(Tuple.of(lFileVersionId.toHex(), lReq.fileId.toHex(), lFileVersion, lSalt));
        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return ;
        } else {
            return ;
        }
    }
}