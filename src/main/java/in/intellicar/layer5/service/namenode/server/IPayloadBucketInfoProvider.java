package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.IBucketRelatedIdInfoProvider;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;

public interface IPayloadBucketInfoProvider
{
    public BucketInfo getBucketForPayload(StorageClsMetaPayload lPayload);
}
