package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;
import in.intellicar.layer5.utils.sha.SHA256Item;

public interface IBucketEditor {
    public void addBucket(BucketInfo lBucket);
    public void removeBucket(BucketInfo lBucket);
    public void splitBucket(SHA256Item lSplitId);
}
