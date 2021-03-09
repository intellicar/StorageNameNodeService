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

        NameNodeClient client = new NameNodeClient("192.168.0.116", 10107, lVertx, logger);
        client.startClient();
        Thread clientThread = new Thread(client);
        clientThread.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request("/clientreqhandler", req);

        while (true) {
            synchronized (future) {

                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.succeeded()) {
                try {
                    clientThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return (AssociatedInstanceIdRsp) future.result().body();
            } else {
                try {
                    clientThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Throwable cause = future.cause();;
                if(cause != null)
                    logger.info("getInstanceId failed with error: " + cause.getMessage());
                else
                    logger.info("getInstanceID failed");
                return null;
            }
        }
    }

    public static Future<SHA256Item> checkAccountID(String accountName, MySQLPool vertxMySQLClient, Logger logger) {

        //TODO update db.table_name
        String sql = "SELECT account_id from accounts.account_info where account_name = '" + accountName + "'";
        Future<RowSet<Row>> future = vertxMySQLClient
                .query(sql)
                .execute();
        while (true) {
            synchronized (future) {

                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.isComplete()) {
                RowSet<Row> rows = future.result();
                if (rows != null && rows.size() > 0) {
                    String hexID = rows.iterator().next().getString("account_id"); // TODO update column name
                    SHA256Item accountIDSHA = new SHA256Item(LittleEndianUtils.hexStringToByteArray(hexID));
                    return Future.succeededFuture(accountIDSHA);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
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

        String ipString = Integer.toString(((int)ipBytes[0])) + "." + Integer.toString(((int)ipBytes[1])) + "." +
                Integer.toString(((int)ipBytes[2])) + "." + Integer.toString(((int)ipBytes[3]));
        NameNodeClient client = new NameNodeClient(ipString, lInstanceIdRsp.port, lVertx, lLogger);
        client.startClient();
        Thread clientThread = new Thread(client);
        clientThread.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request("/clientreqhandler", accIdRegReq);

        while (true) {
            synchronized (future) {

                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.succeeded()) {
                try {
                    clientThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return (AccIdRegisterRsp) future.result().body();
            } else {
                try {
                    clientThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Throwable cause = future.cause();;
                if(cause != null)
                    lLogger.info("getInstanceId failed with error: " + cause.getMessage());
                else
                    lLogger.info("getInstanceID failed");
                return null;
            }
        }
    }

    public static Future<SHA256Item> getAccountID(AccIdGenerateReq req, Vertx lVertx, MySQLPool vertxMySQLClient, Logger logger){
        String accountName = LittleEndianUtils.printHexArray(req.accNameUtf8Bytes);

        Future<SHA256Item> checkedAccountID = checkAccountID(accountName, vertxMySQLClient, logger);
        if (checkedAccountID.succeeded()) {
            return checkedAccountID;
        } else {
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item accountIDSHA = generateAccountId(req.accNameUtf8Bytes, salt.getBytes());

            //TODO: Update db.table_name
            Future<RowSet<Row>> insertFuture = vertxMySQLClient.preparedQuery("INSERT INTO accounts.account_info (account_name, salt, account_id, ack) values (?, ?, ?, ?)")
                    .execute(Tuple.of(accountName, salt, accountIDSHA.toHex(), 0));

            while (true) {
                synchronized (insertFuture) {

                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    AssociatedInstanceIdRsp instanceIdRsp = getInstanceID(lVertx, accountIDSHA, logger);
                    AccIdRegisterRsp accIdRegisterRsp = sendRegisterAccountIdRequest(instanceIdRsp, accountIDSHA, accountName, salt, lVertx, logger);
                    return Future.succeededFuture(accountIDSHA);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        }
    }

    //TODO
    public static Future<AccIdRegisterRsp> registerAccountID(AccIdRegisterReq req,  MySQLPool vertxMySQLClient, Logger logger){
        return null;
    }


    public static Future<Integer> getAck() {
        return Future.succeededFuture(1);
    }


    public static Future<SHA256Item> checkNamespaceId(NsIdGenerateReq req, MySQLPool vertxMySQLClient, Logger logger) {
        String namespaceName = new String(req.namespaceBytes, StandardCharsets.UTF_8);
        String sql = "SELECT namespace_id from accounts.namespaceinfo where namespace_name = '" + namespaceName + "' and account_id = '" + req.accountID.toHex() + "'";
        Future<RowSet<Row>> future = vertxMySQLClient
                .query(sql)
                .execute();
        while (true) {
            synchronized (future) {
                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.isComplete()) {
                RowSet<Row> rows = future.result();
                if (rows != null && rows.size() > 0) {
                    String idHexString = rows.iterator().next().getString("namespace_id");
                    SHA256Item namespaceID = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                    return Future.succeededFuture(namespaceID);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
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
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventBus eventBus = lVertx.eventBus();
        Future<Message<StorageClsMetaPayload>> future = eventBus.request("/clientreqhandler", nsIdRegReq);

        while (true) {
            synchronized (future) {

                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.succeeded()) {
                try {
                    clientThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return (NsIdRegisterRsp) future.result().body();
            } else {
                try {
                    clientThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Throwable cause = future.cause();;
                if(cause != null)
                    lLogger.info("getInstanceId failed with error: " + cause.getMessage());
                else
                    lLogger.info("getInstanceID failed");
                return null;
            }
        }
    }

    public static Future<SHA256Item> getNamespaceId(NsIdGenerateReq lReq, Vertx lVertx, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<SHA256Item> existingNsIdFuture = checkNamespaceId(lReq, lVertxMySQLClient, lLogger);
        if (existingNsIdFuture.succeeded()) {
            return existingNsIdFuture;
        } else {
            String namespaceName = new String(lReq.namespaceBytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item namespaceID = generateNamespaceId(lReq.accountID.hashdata, lReq.namespaceBytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.namespaceinfo (account_id, namespace_name, salt, namespace_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.accountID.toHex(), namespaceName, salt, namespaceID.toHex()));

            while (true) {
                synchronized (insertFuture) {
                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    AssociatedInstanceIdRsp instanceIdRsp = getInstanceID(lVertx, namespaceID, lLogger);
                    NsIdRegisterRsp nsIdRegisterRsp = sendRegisterNsIdRequest(instanceIdRsp, namespaceID, lReq.accountID, namespaceName, salt, lVertx, lLogger);
                    return Future.succeededFuture(namespaceID);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
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
        while (true) {
            synchronized (future) {
                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.isComplete()) {
                RowSet<Row> rows = future.result();
                if (rows != null && rows.size() > 0) {
                    String idHexString = rows.iterator().next().getString("dir_id");
                    SHA256Item dirId = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                    return Future.succeededFuture(dirId);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
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
        if (existingDirIdFuture.succeeded()) {
            return existingDirIdFuture;
        } else {
            String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item dirId = generateDirId(lReq.nsId.hashdata, lReq.hasParentDir == 1,
                    lReq.parentDirId.hashdata, lReq.dirNameUtf8Bytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.directoryinfo (ns_id, parentdir_id, dir_name, salt, dir_id) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.nsId.toHex(), (lReq.hasParentDir == 1?lReq.parentDirId.toHex():""), dirName, salt, dirId.toHex()));

            while (true) {
                synchronized (insertFuture) {
                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    registerDirId(dirId, lReq, salt, lVertxMySQLClient);
                    return Future.succeededFuture(dirId);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        }
    }

    public static void registerDirId(SHA256Item lDirId, DirIdGenerateAndRegisterReq lReq, String lSalt, MySQLPool lVertxMySQLClient)
    {
        String dirName = new String(lReq.dirNameUtf8Bytes, StandardCharsets.UTF_8);
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.directoryid_info (ns_id, parentdir_id, dir_name, salt, dir_id) values (?, ?, ?, ?, ?)")
                .execute(Tuple.of(lReq.nsId.toHex(), (lReq.hasParentDir == 1?lReq.parentDirId.toHex():""), dirName, lSalt, lDirId.toHex()));
        while (true) {
            synchronized (insertFuture) {
                try {
                    insertFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (insertFuture.succeeded()) {
                return ;
            } else {
                return ;
            }
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
        while (true) {
            synchronized (future) {
                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.isComplete()) {
                RowSet<Row> rows = future.result();
                if (rows != null && rows.size() > 0) {
                    String idHexString = rows.iterator().next().getString("file_id");
                    SHA256Item dirId = new SHA256Item(LittleEndianUtils.hexStringToByteArray(idHexString));
                    return Future.succeededFuture(dirId);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
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
        if (existingFileIdFuture.succeeded()) {
            return existingFileIdFuture;
        } else {
            String fileName = new String(lReq.fileNameUtf8Bytes, StandardCharsets.UTF_8);
            String salt = Long.toHexString(System.nanoTime());

            SHA256Item fileId = generateFileId(lReq.nsId.hashdata,
                    lReq.parentDirId.hashdata, lReq.fileNameUtf8Bytes, salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.fileinfo (ns_id, parentdir_id, file_name, salt, file_id) values (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.nsId.toHex(), lReq.parentDirId.toHex(), lReq.fileNameUtf8Bytes, salt, fileId.toHex()));

            while (true) {
                synchronized (insertFuture) {
                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    registerFileId(fileId, lReq, salt, lVertxMySQLClient);
                    addFileVersion(fileId, lVertxMySQLClient);//adding file version
                    return Future.succeededFuture(fileId);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        }
    }

    public static void registerFileId(SHA256Item lFileId, FileIdGenerateAndRegisterReq lReq, String lSalt, MySQLPool lVertxMySQLClient)
    {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.fileid_info (ns_id, parentdir_id, file_name, salt, file_id) values (?, ?, ?, ?, ?)")
                .execute(Tuple.of(lReq.nsId.toHex(), lReq.parentDirId.toHex(), lReq.fileNameUtf8Bytes, lSalt, lFileId.toHex()));
        while (true) {
            synchronized (insertFuture) {
                try {
                    insertFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (insertFuture.succeeded()) {
                return ;
            } else {
                return ;
            }
        }
    }
    public static void addFileVersion(SHA256Item lFileId, MySQLPool lVertxMySQLClient)
    {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_info (file_id, file_version) values (?, ?)")
                .execute(Tuple.of(lFileId.toHex(), 0));

        while (true) {
            synchronized (insertFuture) {
                try {
                    insertFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (insertFuture.succeeded()) {
                return;//TODO:: return type need to be added to handle errors
            } else {
                return ;
            }
        }
    }
    public static Future<String> getAndUpdateFileVersionNo(FileVersionIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String sql = "SELECT file_version from accounts.file_version_info where file_id = '" + lReq.fileId.toHex() + "'";
        Future<RowSet<Row>> future = lVertxMySQLClient
                .query(sql)
                .execute();
        while (true) {
            synchronized (future) {
                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.isComplete()) {
                RowSet<Row> rows = future.result();
                if (rows != null && rows.size() > 0) {
                    String fileVersionString = rows.iterator().next().getString("file_version");
                    updateFileVersion(lReq.fileId, fileVersionString, lVertxMySQLClient);
                    Future.succeededFuture(fileVersionString);
                }
            } else {
                return Future.failedFuture(future.cause());
            }
        }
    }

    public static void updateFileVersion(SHA256Item lFileId, String lCurrVersion, MySQLPool lVertxMySQLClient)
    {
        String sql = "Update accounts.file_version_info set file_version = '"+ Integer.parseInt(lCurrVersion) + 1 +"' where file_id = '" + lFileId.toHex() + "'";
        Future<RowSet<Row>> updateFuture = lVertxMySQLClient
                .query(sql)
                .execute();
        while (true) {
            synchronized (updateFuture) {
                try {
                    updateFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (updateFuture.isComplete() && updateFuture.succeeded()) {
                return ;//TODO:: return type need to be added to handle errors
            } else {
                return ;
            }
        }
    }


    public static SHA256Item generateFileVersionId(byte[] lFileIdBytes, byte[] lVersionNoBytes, byte[] salt) {
        ArrayList<byte[]> pathArray = new ArrayList<>();
        pathArray.add(lFileIdBytes);
        pathArray.add(lVersionNoBytes);
        return generateIdForPath(pathArray, salt);
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

    public static Future<SHA256Item> getFileVersionId(FileVersionIdGenerateAndRegisterReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<String> fileVersionFuture = getAndUpdateFileVersionNo(lReq, lVertxMySQLClient, lLogger);
        if (fileVersionFuture.succeeded()) {
            String fileVersion = fileVersionFuture.result();
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item fileVersionId = generateFileVersionId(lReq.fileId.hashdata,
                    fileVersion.getBytes(StandardCharsets.UTF_8), salt.getBytes());
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_id_info (file_id, version, salt, file_version_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(lReq.fileId.toHex(), fileVersion, salt, fileVersionId.toHex()));

            while (true) {
                synchronized (insertFuture) {
                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    registerFileVersionId(fileVersionId, lReq, fileVersion, salt, lVertxMySQLClient);
                    return Future.succeededFuture(fileVersionId);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        } else {
            return Future.failedFuture(fileVersionFuture.cause());
        }
    }

    public static void registerFileVersionId(SHA256Item lFileVersionId, FileVersionIdGenerateAndRegisterReq lReq, String lFileVersion, String lSalt, MySQLPool lVertxMySQLClient)
    {
        Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.file_version_id_r_info (file_id, version, salt, file_version_id) values (?, ?, ?, ?)")
                .execute(Tuple.of(lReq.fileId.toHex(), lFileVersion, lSalt, lFileVersionId.toHex()));
        while (true) {
            synchronized (insertFuture) {
                try {
                    insertFuture.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (insertFuture.succeeded()) {
                return ;
            } else {
                return ;
            }
        }
    }
}