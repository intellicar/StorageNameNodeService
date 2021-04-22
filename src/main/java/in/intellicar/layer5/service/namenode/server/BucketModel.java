package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.SplitBucketReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.IBucketRelatedIdInfoProvider;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;
import in.intellicar.layer5.utils.sha.SHA256Item;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BucketModel implements IPayloadBucketInfoProvider, IBucketEditor{
    private List<BucketInfo> _buckets;
    public BucketModel()
    {
        _buckets = new ArrayList<>();
    }
    @Override
    public BucketInfo getBucketForPayload(StorageClsMetaPayload lPayload) {
        if(lPayload instanceof IBucketRelatedIdInfoProvider)
        {
            return getMatchingBucket(((IBucketRelatedIdInfoProvider)lPayload).getIdReleatedToBucket().toHex());
        }
        else if(lPayload instanceof SplitBucketReq)
        {
            return null;
        }
        return null;
    }

    @Override
    public void addBucket(BucketInfo lBucket) {
        _buckets.add(lBucket);
    }

    @Override
    public void removeBucket(BucketInfo lBucket) {
        _buckets.remove(lBucket);
    }

    @Override
    public void splitBucket(SHA256Item lSplitId)
    {
        //TODO:: lock need to be used around list
        //null check isn't needed as it's called by bucketmanager who already taken care of spliting
        BucketInfo matchedBucket = getMatchingBucket(lSplitId.toHex());
        _buckets.remove(matchedBucket);
        BucketInfo newBucket = new BucketInfo(lSplitId.toHex(), matchedBucket.endBucket.toHex());
        matchedBucket.endBucket = lSplitId;
        _buckets.add(matchedBucket);
        _buckets.add(newBucket);
    }

    private boolean isBucketMatched(String lIdToMatch, BucketInfo lBucket)
    {
        return lIdToMatch.compareToIgnoreCase(lBucket.startBucket.toHex()) > 0
                && lIdToMatch.compareToIgnoreCase(lBucket.endBucket.toHex()) <= 0;
    }

    private BucketInfo getMatchingBucket(String lIdToMatch)
    {
        BucketInfo returnValue = null;
        for(BucketInfo curBucket : _buckets)
        {
            if(isBucketMatched(lIdToMatch, curBucket))
            {
                returnValue = curBucket;
                break;
            }
        }
        return returnValue;
    }

}
