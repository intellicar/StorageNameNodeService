package in.intellicar.layer5.service.namenode.mysql;

import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.*;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.service.common.IPayloadRequestHandler;
import in.intellicar.layer5.service.namenode.utils.NameNodeUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLPool;

import java.util.logging.Logger;

/**
 * @author krishna mohan
 * @version 1.0
 * @project StorageClsAccIDMetaService
 * @date 02/03/21 - 5:09 PM
 */
public class NameNodePayloadHandler implements IPayloadRequestHandler {
    private Vertx _vertx;
    public NameNodePayloadHandler(Vertx lVertx)
    {
        _vertx = lVertx;
    }

    @Override
    public StorageClsMetaPayload getResponsePayload(StorageClsMetaPayload lRequestPayload, MySQLPool lVertxMySQLClient, Logger lLogger) {
        short subType = lRequestPayload.getSubType();
        PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

        switch (payloadType) {
            case ACCOUNT_ID_GEN_REQ:
                Future<AccIdGenerateRsp> accountIDFuture = NameNodeUtils.getAccountID((AccIdGenerateReq) lRequestPayload, _vertx, lVertxMySQLClient, lLogger);
                if (accountIDFuture.isComplete() && accountIDFuture.succeeded()) {
                    AccIdGenerateRsp accIdGenerateRsp = accountIDFuture.result();
                    return accIdGenerateRsp;
                } else {
                    return new StorageClsMetaErrorRsp(accountIDFuture.cause().getLocalizedMessage(), lRequestPayload);
                }
            case ACCOUNT_ID_REG_REQ:
                Future<AccIdRegisterRsp> accIdRegRspFuture = NameNodeUtils.registerAccountID((AccIdRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if (accIdRegRspFuture.succeeded()) {
                    return accIdRegRspFuture.result();
                } else {
                    return new StorageClsMetaErrorRsp(accIdRegRspFuture.cause().getLocalizedMessage(), lRequestPayload);
                }
            case ACCOUNT_ID_REG_RSP://TODO:: using it to update ack for now, might need to change it in future
                NameNodeUtils.updateAckOfAccName((AccIdRegisterRsp) lRequestPayload, lVertxMySQLClient, lLogger);
                return lRequestPayload;
            case NS_ID_GEN_REQ:
                Future<NsIdGenerateRsp> nsIdFuture = NameNodeUtils.getNamespaceId((NsIdGenerateReq) lRequestPayload, _vertx, lVertxMySQLClient, lLogger);
                if(nsIdFuture.isComplete() && nsIdFuture.succeeded())
                {
                    NsIdGenerateRsp nsIdGenerateRsp = nsIdFuture.result();
                    return nsIdGenerateRsp;
                }
                return new StorageClsMetaErrorRsp(nsIdFuture.cause().getMessage(), lRequestPayload);

            case NS_ID_REG_REQ:
                Future<NsIdRegisterRsp> nsIdRegRspFuture = NameNodeUtils.registerNsId((NsIdRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if (nsIdRegRspFuture.succeeded()) {
                    return nsIdRegRspFuture.result();
                } else {
                    return new StorageClsMetaErrorRsp(nsIdRegRspFuture.cause().getLocalizedMessage(), lRequestPayload);
                }
            case NS_ID_REG_RSP://TODO:: using it to update ack for now, might need to change it in future
                NameNodeUtils.updateAckOfNsName((NsIdRegisterRsp) lRequestPayload, lVertxMySQLClient, lLogger);
                return lRequestPayload;
            case DIR_ID_GEN_REG_REQ:
                Future<SHA256Item> dirIdFuture = NameNodeUtils.getDirId((DirIdGenerateAndRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if(dirIdFuture.isComplete() && dirIdFuture.succeeded())
                {
                    SHA256Item dirId = dirIdFuture.result();
                    return new DirIdGenerateAndRegisterRsp((DirIdGenerateAndRegisterReq) lRequestPayload, dirId);
                }
                return new StorageClsMetaErrorRsp(dirIdFuture.cause().getMessage(), lRequestPayload);
            case FILE_ID_GEN_REG_REQ:
                Future<SHA256Item> fileIdFuture = NameNodeUtils.getFileId((FileIdGenerateAndRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if(fileIdFuture.isComplete() && fileIdFuture.succeeded())
                {
                    SHA256Item fileId = fileIdFuture.result();
                    return new FileIdGenerateAndRegisterRsp((FileIdGenerateAndRegisterReq) lRequestPayload, fileId);
                }
                return new StorageClsMetaErrorRsp(fileIdFuture.cause().getMessage(), lRequestPayload);
            case FILE_VERSION_ID_GEN_REG_REQ:
                Future<SHA256Item> fileVersionIdFuture = NameNodeUtils.getFileVersionId((FileVersionIdGenerateAndRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if(fileVersionIdFuture.isComplete() && fileVersionIdFuture.succeeded())
                {
                    SHA256Item fileVersionId = fileVersionIdFuture.result();
                    return new FileVersionIdGenerateAndRegisterRsp((FileVersionIdGenerateAndRegisterReq) lRequestPayload, fileVersionId);
                }
                return new StorageClsMetaErrorRsp(fileVersionIdFuture.cause().getMessage(), lRequestPayload);

            default:
                return new StorageClsMetaErrorRsp("Sent Unknown PayloadType",  lRequestPayload);
        }
    }
}
