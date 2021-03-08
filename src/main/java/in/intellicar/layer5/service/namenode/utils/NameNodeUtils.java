package in.intellicar.layer5.service.namenode.utils;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.AssociatedInstanceIdReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.AccIdGenerateReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.AccIdGenerateRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterRsp;
import in.intellicar.layer5.service.namenode.client.NameNodeClient;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import in.intellicar.layer5.utils.sha.SHA256Utils;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

public class NameNodeUtils {

    //TODO
    public static Future<SHA256Item> generateAccountID(AccIdGenerateReq req, MySQLPool vertxMySQLClient, Logger logger){
        String accountName = LittleEndianUtils.printHexArray(req.accNameUtf8Bytes);

        Future<SHA256Item> checkedAccountID = checkAccountID(accountName, vertxMySQLClient, logger);
        if (checkedAccountID.succeeded()) {
            return checkedAccountID;
        } else {
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item accountIDSHA = generateAccountID(req.accNameUtf8Bytes, salt.getBytes());

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


    // Utility Functions
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

    public static SHA256Item generateAccountID(byte[] nameBytes, byte[] saltBytes) {

        byte[] saltyName = new byte[saltBytes.length + nameBytes.length];
        System.arraycopy(nameBytes, 0, saltyName, 0, nameBytes.length);
        System.arraycopy(saltBytes, 0, saltyName, nameBytes.length, saltBytes.length);

        try {
            return SHA256Utils.getSHA256(saltyName);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

/*
TODO: Client End
1. Get InstanceId corresponding to accountName
2. Send AccIdRegisterReq to InstanceId
**/
    public static Future<SHA256Item> getInstanceID(SHA256Item accountID, int seqID,  Logger logger) throws InterruptedException {

        AssociatedInstanceIdReq req = new AssociatedInstanceIdReq(accountID);
        StorageClsMetaBeacon beacon = new StorageClsMetaBeacon(seqID, req);

        NameNodeClient client = new NameNodeClient("192.168.0.116", 10107, "/server/naveen/mb15", beacon, logger);
        client.startClient();
        Thread clientThread = new Thread(client);
        clientThread.start();
        clientThread.join();
        return null;
    }

    public static Future<Integer> getAck() {
        return Future.succeededFuture(1);
    }

}
