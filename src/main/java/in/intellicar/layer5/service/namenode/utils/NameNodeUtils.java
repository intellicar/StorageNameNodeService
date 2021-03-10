package in.intellicar.layer5.service.namenode.utils;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
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
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class NameNodeUtils {

    // Utility Functions
    /*
    TODO: Client End
    1. Get InstanceId corresponding to accountName
    2. Send AccIdRegisterReq to InstanceId
    **/
    public static AssociatedInstanceIdRsp getInstanceID(Vertx lVertx, SHA256Item lIdToBeMatched, Logger logger) {

        AssociatedInstanceIdReq req = new AssociatedInstanceIdReq(lIdToBeMatched);
        //StorageClsMetaBeacon beacon = new StorageClsMetaBeacon(seqID, req);

        NameNodeClient client = new NameNodeClient("localhost", 10107, lVertx, logger);
        Thread clientThread = new Thread(client);
        clientThread.start();
        AssociatedInstanceIdRsp resultValue = null;
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request("/clientreqhandler", req);
        doWaitOnFuture(future);
        if (future.succeeded()) {
            resultValue = (AssociatedInstanceIdRsp) future.result().body();
        } else {
            Throwable cause = future.cause();
            ;
            if (cause != null)
                logger.info("getInstanceId failed with error: " + cause.getMessage());
            else
                logger.info("getInstanceID failed");
            resultValue = null;
        }
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return resultValue;
    }

    private static <T> void doWaitOnFuture(Future<T> lFuture)
    {
        while (true) {
            synchronized (lFuture) {

                try {
                    lFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(lFuture.isComplete()) {
                return;
            }
        }
    }

    /**
     * This method will be used where ever we need to generate Ids. Individual generate methods will be replaced by this method
     * @param lIdsAndStringsMakingAbsPath
     * @param salt
     * @return
     */
    public static SHA256Item generateIdForPath(List<byte[]>lIdsAndStringsMakingAbsPath, byte[] salt)
    {
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

    public static Future<SHA256Item> checkAccountID(String accountName, MySQLPool vertxMySQLClient, Logger logger) {

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
                SHA256Item accountIDSHA = new SHA256Item(LittleEndianUtils.hexStringToByteArray(hexID));
                return Future.succeededFuture(accountIDSHA);
            } else {
                return Future.failedFuture(new Throwable("succeeded, but rows aren't correct"));
            }
        }
        else {
            return Future.failedFuture(future.cause());
        }
    }

    public static SHA256Item generateAccountId(byte[] nameBytes, byte[] saltBytes) {

        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(nameBytes);
        return generateIdForPath(pathArray, saltBytes);
    }

    public static AccIdRegisterRsp sendRegisterAccountIdRequest(AssociatedInstanceIdRsp lInstanceIdRsp, SHA256Item lAccId, String lAccName, String lSalt, Vertx lVertx, Logger lLogger)
    {
        AccIdRegisterReq accIdRegReq = new AccIdRegisterReq(lAccId, lAccName.getBytes(StandardCharsets.UTF_8), lSalt.getBytes(StandardCharsets.UTF_8));
        byte[] ipBytes = lInstanceIdRsp.ip;

        String ipString = Byte.toUnsignedInt(ipBytes[0]) + "." + Byte.toUnsignedInt(ipBytes[1]) + "." +
                Byte.toUnsignedInt(ipBytes[2]) + "." + Byte.toUnsignedInt(ipBytes[3]);
        System.out.println("Port: " + lInstanceIdRsp.port);
        System.out.println("Host: " + ipString);

        NameNodeClient client = new NameNodeClient(ipString, lInstanceIdRsp.port, lVertx, lLogger);
        Thread clientThread = new Thread(client);
        clientThread.start();
        AccIdRegisterRsp returnValue = null;
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request("/clientreqhandler", accIdRegReq);

        doWaitOnFuture(future);
        if (future.succeeded()) {
            returnValue = (AccIdRegisterRsp) future.result().body();
        } else {
            Throwable cause = future.cause();;
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

    private static void updateAckOfAccName(AccIdRegisterRsp lRsp, MySQLPool lVertxMySQLClient, Logger logger)
    {
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

    public static Future<SHA256Item> getAccountID(AccIdGenerateReq req, Vertx lVertx, MySQLPool vertxMySQLClient, Logger logger){
        String accountName = new String(req.accNameUtf8Bytes, StandardCharsets.UTF_8);
        Future<SHA256Item> checkedAccountIDFuture = checkAccountID(accountName, vertxMySQLClient, logger);
//        doWaitOnFuture(checkedAccountIDFuture);
        if (checkedAccountIDFuture.succeeded()) {
            return checkedAccountIDFuture;
        } else {
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item accountIDSHA = generateAccountId(req.accNameUtf8Bytes, salt.getBytes());

            //TODO: Update db.table_name
            Future<RowSet<Row>> insertFuture = vertxMySQLClient.preparedQuery("INSERT INTO accounts.account_info (account_name, salt, account_id, ack) values (?, ?, ?, ?)")
                    .execute(Tuple.of(accountName, salt, accountIDSHA.toHex(), 0));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                AssociatedInstanceIdRsp instanceIdRsp = getInstanceID(lVertx, accountIDSHA, logger);
                AccIdRegisterRsp accIdRegisterRsp = sendRegisterAccountIdRequest(instanceIdRsp, accountIDSHA, accountName, salt, lVertx, logger);
                updateAckOfAccName(accIdRegisterRsp, vertxMySQLClient, logger);
                return Future.succeededFuture(accountIDSHA);
            } else {
                return Future.failedFuture(insertFuture.cause());
            }
        }
    }

    public static Future<AccIdRegisterRsp> registerAccountID(AccIdRegisterReq lReq,  MySQLPool lVertxMySQLClient, Logger lLogger){
        String accountName = new String(lReq.accountNameUtf8Bytes, StandardCharsets.UTF_8);
        String saltString = new String(lReq.saltBytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.account_id_info (account_id, account_name, salt) values (?, ?, ?)")
                .execute(Tuple.of(lReq.accountId.toHex(), accountName, saltString));

        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            AccIdRegisterRsp accIdRegisterRsp = new AccIdRegisterRsp(lReq, 1);
            return Future.succeededFuture(accIdRegisterRsp);
        } else {
            return Future.failedFuture(insertFuture.cause());
        }
    }

    public static Future<SHA256Item> checkNamespaceId(NsIdGenerateReq req, MySQLPool vertxMySQLClient, Logger logger) {
        String namespaceName = new String(req.namespaceBytes, StandardCharsets.UTF_8);
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
                return Future.succeededFuture(namespaceID);
            } else {
                return Future.failedFuture(new Throwable("succeeded, but rows aren't correct"));
            }
        }
        else{
            return Future.failedFuture(future.cause());
        }
    }

    public static SHA256Item generateNamespaceId(byte[] accountIDbytes, byte[] nameBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(accountIDbytes);
        pathArray.add(nameBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static NsIdRegisterRsp sendRegisterNsIdRequest(AssociatedInstanceIdRsp lInstanceIdRsp, SHA256Item lNsId, SHA256Item lAccId, String lNsName, String lSalt, Vertx lVertx, Logger lLogger)
    {
        NsIdRegisterReq nsIdRegReq = new NsIdRegisterReq(lNsId, lAccId, lNsName.getBytes(StandardCharsets.UTF_8), lSalt.getBytes(StandardCharsets.UTF_8));
        byte[] ipBytes = lInstanceIdRsp.ip;

        String ipString = Integer.toString(((int)ipBytes[0])) + "." + Integer.toString(((int)ipBytes[1])) + "." +
                Integer.toString(((int)ipBytes[2])) + "." + Integer.toString(((int)ipBytes[3]));
        NameNodeClient client = new NameNodeClient(ipString, lInstanceIdRsp.port, lVertx, lLogger);
        client.startClient();
        Thread clientThread = new Thread(client);
        clientThread.start();
        NsIdRegisterRsp returnValue = null;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request("/clientreqhandler", nsIdRegReq);

        doWaitOnFuture(future);
        if (future.succeeded()) {
            returnValue = (NsIdRegisterRsp) future.result().body();
        } else {
            Throwable cause = future.cause();;
            if(cause != null)
                lLogger.info("getInstanceId failed with error: " + cause.getMessage());
            else
                lLogger.info("getInstanceID failed");;
        }
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    private static void updateAckOfNsName(NsIdRegisterRsp lRsp, MySQLPool lVertxMySQLClient, Logger logger)
    {
        String nsName = new String(lRsp.nsIdRegisterReq.nsNameUtf8Bytes, StandardCharsets.UTF_8);
        String sql = "Update accounts.namespaceinfo set ack = '"+ lRsp.ackFlag +"' where namespace_name = '" + nsName +
                "' and account_id = '" + lRsp.nsIdRegisterReq.accountId.toHex();
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

    public static Future<SHA256Item> getNamespaceId(NsIdGenerateReq lReq, Vertx lVertx, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<SHA256Item> existingNsIdFuture = checkNamespaceId(lReq, lVertxMySQLClient, lLogger);
        doWaitOnFuture(existingNsIdFuture);
        if (existingNsIdFuture.succeeded()) {
            return existingNsIdFuture;
        } else {
            String namespaceName = new String(lReq.namespaceBytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item namespaceID = generateNamespaceId(lReq.accountID.hashdata, lReq.namespaceBytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.namespaceinfo (account_id, namespace_name, salt, namespace_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.accountID.toHex(), namespaceName, salt, namespaceID.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                AssociatedInstanceIdRsp instanceIdRsp = getInstanceID(lVertx, namespaceID, lLogger);
                NsIdRegisterRsp nsIdRegisterRsp = sendRegisterNsIdRequest(instanceIdRsp, namespaceID, lReq.accountID, namespaceName, salt, lVertx, lLogger);
                updateAckOfNsName(nsIdRegisterRsp, lVertxMySQLClient, lLogger);
                return Future.succeededFuture(namespaceID);
            } else {
                return Future.failedFuture(insertFuture.cause());
            }
        }
    }

    public static Future<NsIdRegisterRsp> registerNsId(NsIdRegisterReq lReq,  MySQLPool lVertxMySQLClient, Logger lLogger){
        String nsName = new String(lReq.nsNameUtf8Bytes, StandardCharsets.UTF_8);
        String saltString = new String(lReq.saltBytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.namespace_id_info (namespace_id, account_id, namespace_name, salt) values (?, ?, ?, ?)")
                .execute(Tuple.of(lReq.nsId.toHex(), lReq.accountId.toHex(), nsName, saltString));

        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            NsIdRegisterRsp nsIdRegisterRsp = new NsIdRegisterRsp(lReq, 1);
            return Future.succeededFuture(nsIdRegisterRsp);
        } else {
            return Future.failedFuture(insertFuture.cause());
        }
    }

    public static Future<SHA256Item> checkDirId(DirIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
        String sql = "SELECT dir_id from accounts.directoryinfo where ns_id = '" + lReq.nsId.toHex() +
                "' and parentdir_id = '" + (lReq.hasParentDir == 1 ? lReq.parentDirId.toHex(): "") +
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
                return Future.succeededFuture(dirId);
            } else {
                return Future.failedFuture(new Throwable("succeeded, but rows aren't correct"));
            }
        }
        else
        {
            return Future.failedFuture(future.cause());
        }
    }

    public static SHA256Item generateDirId(byte[] lNsIdBytes, boolean lHasParentDir, byte[] lParentDirid, byte[] lDirNameBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(lNsIdBytes);
        if(lHasParentDir)
            pathArray.add(lParentDirid);
        pathArray.add(lDirNameBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static Future<SHA256Item> getDirId(DirIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<SHA256Item> existingDirIdFuture = checkDirId(lReq, lVertxMySQLClient, lLogger);
        doWaitOnFuture(existingDirIdFuture);
        if (existingDirIdFuture.succeeded()) {
            return existingDirIdFuture;
        } else {
            String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item dirId = generateDirId(lReq.nsId.hashdata, lReq.hasParentDir == 1,
                    lReq.parentDirId.hashdata, lReq.dirNameUtf8Bytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.directoryinfo (ns_id, parentdir_id, dir_name, salt, dir_id) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.nsId.toHex(), (lReq.hasParentDir == 1?lReq.parentDirId.toHex():""), dirName, salt, dirId.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                registerDirId(dirId, lReq, salt, lVertxMySQLClient);
                return Future.succeededFuture(dirId);
            } else {
                return Future.failedFuture(insertFuture.cause());
            }
        }
    }

    public static void registerDirId(SHA256Item lDirId, DirIdGenerateAndRegisterReq lReq, String lSalt, MySQLPool lVertxMySQLClient)
    {
        String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.directoryid_info (dir_id, ns_id, parentdir_id, dir_name, salt) values (?, ?, ?, ?, ?)")
                .execute(Tuple.of(lDirId.toHex(), lReq.nsId.toHex(), (lReq.hasParentDir == 1?lReq.parentDirId.toHex():""), dirName, lSalt));
        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return ;
        } else {
            return ;
        }
    }

    public static Future<SHA256Item> checkFileId(FileIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
        String sql = "SELECT file_id from accounts.fileinfo where ns_id = '" + lReq.nsId.toHex() +
                "' and parentdir_id = '" + lReq.parentDirId.toHex() +
                "' and file_name = '" + fileName + "'";
        Future<RowSet<Row>> future = lVertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                String idHexString = rows.iterator().next().getString("file_id");
                SHA256Item dirId = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                return Future.succeededFuture(dirId);
            } else {
                return Future.failedFuture(new Throwable("succeeded, but rows aren't correct"));
            }
        }
        else
        {
            return Future.failedFuture(future.cause());
        }
    }

    public static SHA256Item generateFileId(byte[] lNsIdBytes, byte[] lParentDirid, byte[] lFileNameBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(lNsIdBytes);
        pathArray.add(lParentDirid);
        pathArray.add(lFileNameBytes);
        return generateIdForPath(pathArray, salt);
    }

    public static Future<SHA256Item> getFileId(FileIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<SHA256Item> existingFileIdFuture = checkFileId(lReq, lVertxMySQLClient, lLogger);
        doWaitOnFuture(existingFileIdFuture);
        if (existingFileIdFuture.succeeded()) {
            return existingFileIdFuture;
        } else {
            String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item fileId = generateFileId(lReq.nsId.hashdata,
                    lReq.parentDirId.hashdata, lReq.fileNameUtf8Bytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.fileinfo (ns_id, parentdir_id, file_name, salt, file_id) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.nsId.toHex(), lReq.parentDirId.toHex(), fileName, salt, fileId.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                registerFileId(fileId, lReq, salt, lVertxMySQLClient);
                addFileVersion(fileId, lVertxMySQLClient);//adding file version
                return Future.succeededFuture(fileId);
            } else {
                return Future.failedFuture(insertFuture.cause());
            }
        }
    }

    public static void registerFileId(SHA256Item lFileId, FileIdGenerateAndRegisterReq lReq, String lSalt, MySQLPool lVertxMySQLClient)
    {
        String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.fileid_info (file_id, ns_id, parentdir_id, file_name, salt) values (?, ?, ?, ?, ?)")
                .execute(Tuple.of(lFileId.toHex(), lReq.nsId.toHex(), lReq.parentDirId.toHex(), fileName, lSalt));
        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return ;
        } else {
            return ;
        }
    }
    public static void addFileVersion(SHA256Item lFileId, MySQLPool lVertxMySQLClient)
    {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_info (file_id, file_version) values (?, ?)")
                .execute(Tuple.of(lFileId.toHex(), 0));

        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return;//TODO:: return type need to be added to handle errors
        } else {
            return ;
        }
    }
    public static Future<String> getAndUpdateFileVersionNo(FileVersionIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String sql = "SELECT file_version from accounts.file_version_info where file_id = '" + lReq.fileId.toHex() + "'";
        Future<RowSet<Row>> future = lVertxMySQLClient
                .query(sql)
                .execute();
        doWaitOnFuture(future);
        if (future.succeeded()) {
            RowSet<Row> rows = future.result();
            if (rows != null && rows.size() > 0) {
                String fileVersionString = rows.iterator().next().getString("file_version");
                updateFileVersion(lReq.fileId, fileVersionString, lVertxMySQLClient);
                return Future.succeededFuture(fileVersionString);
            }
            else
            {
                return Future.failedFuture(new Throwable("succeeded, but rows aren't correct"));
            }
        } else {
            return Future.failedFuture(future.cause());
        }
    }

    public static void updateFileVersion(SHA256Item lFileId, String lCurrVersion, MySQLPool lVertxMySQLClient)
    {
        String sql = "Update accounts.file_version_info set file_version = '"+ Integer.parseInt(lCurrVersion) + 1 +"' where file_id = '" + lFileId.toHex() + "'";
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

    public static Future<SHA256Item> getFileVersionId(FileVersionIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<String> fileVersionFuture = getAndUpdateFileVersionNo(lReq, lVertxMySQLClient, lLogger);
        doWaitOnFuture(fileVersionFuture);
        if (fileVersionFuture.succeeded()) {
            String fileVersion = fileVersionFuture.result();
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item fileVersionId = generateFileVersionId(lReq.fileId.hashdata,
                    fileVersion.getBytes(StandardCharsets.UTF_8), salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_id_info (file_id, version, salt, file_version_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.fileId.toHex(), fileVersion, salt, fileVersionId.toHex()));

            doWaitOnFuture(insertFuture);
            if (insertFuture.succeeded()) {
                registerFileVersionId(fileVersionId, lReq, fileVersion, salt, lVertxMySQLClient);
                return Future.succeededFuture(fileVersionId);
            } else {
                return Future.failedFuture(insertFuture.cause());
            }
        } else {
            return Future.failedFuture(fileVersionFuture.cause());
        }
    }

    public static void registerFileVersionId(SHA256Item lFileVersionId, FileVersionIdGenerateAndRegisterReq lReq, String lFileVersion, String lSalt, MySQLPool lVertxMySQLClient)
    {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_id_reg_info (file_version_id, file_id, version, salt) values (?, ?, ?, ?)")
                .execute(Tuple.of(lFileVersionId.toHex(), lReq.fileId.toHex(), lFileVersion, lSalt));
        doWaitOnFuture(insertFuture);
        if (insertFuture.succeeded()) {
            return ;
        } else {
            return ;
        }
    }
}