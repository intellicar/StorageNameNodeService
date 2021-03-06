package in.intellicar.layer5.service.namenode.mysql;

import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.beacon.storagemetacls.service.common.IPayloadRequestHandler;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLPool;

import java.util.logging.Logger;

/**
 * @author krishna mohan
 * @version 1.0
 * @project StorageClsAccIDMetaService
 * @date 02/03/21 - 5:09 PM
 */
public class NameNodePayloadHandler implements IPayloadRequestHandler {
    @Override
    public StorageClsMetaPayload getResponsePayload(StorageClsMetaPayload lRequestPayload, MySQLPool lVertxMySQLClient, Logger lLogger) {
        short subType = lRequestPayload.getSubType();
        PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

        switch (payloadType) {
            case ACCOUNT_ID_GEN_REQ:
//                Future<SHA256Item> accountIDFuture = NameNodeUtils.getAccountID((AccountIDReq) lRequestPayload, lVertxMySQLClient, lLogger);
//                if (accountIDFuture.succeeded()) {
//                    return new AccountIDRsp((AccountIDReq) lRequestPayload, accountIDFuture.result());
//                } else {
//                    return new StorageClsMetaErrorRsp(accountIDFuture.cause().getLocalizedMessage(), lRequestPayload);
//                }
                return null;
            case ACCOUNT_ID_REG_REQ:
//                Future<SHA256Item> nsIDFuture = NameNodeUtils.getNsId((NsIdReq) lRequestPayload, lVertxMySQLClient, lLogger);
//                if (nsIDFuture.succeeded()) {
//                    return new NsIdRsp((NsIdReq) lRequestPayload, nsIDFuture.result());
//                } else {
//                    return new StorageClsMetaErrorRsp(nsIDFuture.cause().getLocalizedMessage(), lRequestPayload);
//                }
                return null;
            default:
                return new StorageClsMetaErrorRsp("Sent Unknown PayloadType",  lRequestPayload);
        }
    }
}
