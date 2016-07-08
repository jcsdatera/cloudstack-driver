package org.apache.cloudstack.storage.test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cloudstack.storage.datastore.utils.AppInstanceInfo;
import org.apache.cloudstack.storage.datastore.utils.DateraModel;
import org.apache.cloudstack.storage.datastore.utils.DateraRestClient;
import org.apache.cloudstack.storage.datastore.utils.DateraUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DateraSharedStorageTests {

    @Test
    public void utValidateNetworkPoolName()
    {
        
        DateraRestClient rest = new DateraRestClient(DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, DateraCommon.PASSWORD);
        List<String> poolNames = rest.enumerateNetworkPool();
        assertTrue(poolNames.size()>0);
        assertTrue(poolNames.contains(DateraCommon.DEFAULT_NETWORK_POOL_NAME));//for the default storage pool name
        
    }

    @Test
    public void utRegisterInitiators()
    {

        DateraRestClient rest = new DateraRestClient(DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, DateraCommon.PASSWORD);
        List<String> initiators = registerInitiators(rest);
        unRegisterInitiators(rest, initiators);
    }

    @Test
    public void utRegisterInitiatorGroup()
    {
        DateraRestClient rest = new DateraRestClient(DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, DateraCommon.PASSWORD);
        List<String> initiators = registerInitiators(rest);

        List<String> initiatorGroups;
        String initiatorGroupName = createInitiatorGroup(rest, initiators);
        
        deleteInitiatorGroup(rest, initiatorGroupName);
        
        unRegisterInitiators(rest, initiators);
    }

    private void deleteInitiatorGroup(DateraRestClient rest,
            String initiatorGroupName) {
        List<String> initiatorGroups;
        rest.deleteInitiatorGroup(initiatorGroupName);
        initiatorGroups = rest.enumerateInitiatorGroups();
        assertEquals(false, initiatorGroups.contains(initiatorGroupName));
    }

    private String createInitiatorGroup(DateraRestClient rest,
            List<String> initiators) {
        String initiatorGroupName = "myGroup";
        List<String> initiatorGroups = rest.enumerateInitiatorGroups();
        for(;;)
        {
            if(false == initiatorGroups.contains(initiatorGroupName))
            {
                break;
            }
            else
            {
                initiatorGroupName = initiatorGroupName+Math.random();
            }
        }
        rest.createInitiatorGroup(initiatorGroupName, initiators);
          
        initiatorGroups = rest.enumerateInitiatorGroups();
        assertEquals(true, initiatorGroups.contains(initiatorGroupName));
        return initiatorGroupName;
    }
    @Test
    public void utRegisterPrimaryStorage() 
    {
        String appInstanceName = DateraCommon.generateAppName();
        String networkPoolName = DateraCommon.DEFAULT_NETWORK_POOL_NAME;
        long capacityBytes = DateraCommon.DEFAULT_CAPACITY_BYTES;
        int replica = DateraCommon.DEFAULT_REPLICA;
        long totalIOPS = DateraCommon.DEFAULT_CAPACITY_IOPS;
        String accessControlMode = "allow_all";
        int dtVolSize = DateraUtil.getVolumeSizeInGB(capacityBytes);

        DateraRestClient rest = new DateraRestClient(DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, DateraCommon.PASSWORD);
        assertEquals(false,rest.isAppInstanceExists(appInstanceName));
        assertEquals(true,rest.enumerateNetworkPool().contains(networkPoolName));
        
        String storageInstanceName = DateraModel.defaultStorageName;
        String volumeInstanceName = DateraModel.defaultVolumeName;
        
        rest.createVolume(appInstanceName, null, null, dtVolSize, replica, accessControlMode, networkPoolName);
        AppInstanceInfo.VolumeInfo volInfo = rest.getVolumeInfo(appInstanceName, storageInstanceName, volumeInstanceName);
        assertTrue(volInfo.name.equals(volumeInstanceName));
        //System.out.println("OPSTATE : " + volInfo.opState.compareTo(DateraRestClient.OP_STATE_AVAILABLE));
       // assertTrue(0 == volInfo.opState.compareTo(DateraRestClient.OP_STATE_AVAILABLE));
        

        AppInstanceInfo.StorageInstance storageInfo = rest.getStorageInfo(appInstanceName, storageInstanceName);
        assertEquals(dtVolSize, storageInfo.volumes.volume1.size);
        assertEquals(replica, storageInfo.volumes.volume1.replicaCount);
        assertTrue(storageInfo.ipPool.contains(networkPoolName));
        assertEquals(0,storageInfo.aclPolicy.initiatorGroups.size());

        List<String> initiators = registerInitiators(rest);
        String initiatorGroupName = createInitiatorGroup(rest, initiators);
        List<String> initiatorGroups = new ArrayList<String>();
        initiatorGroups.add(initiatorGroupName);
        rest.updateStorageWithInitiator(appInstanceName, storageInstanceName, null, initiatorGroups);
        storageInfo = rest.getStorageInfo(appInstanceName, storageInstanceName);
        assertEquals(1,storageInfo.aclPolicy.initiatorGroups.size());
        assertTrue(storageInfo.aclPolicy.initiatorGroups.get(0).contains(initiatorGroupName));
        
        deleteAppInstance(appInstanceName, rest);
        assertEquals(false,rest.isAppInstanceExists(appInstanceName));
        
        deleteInitiatorGroup(rest, initiatorGroupName);
        unRegisterInitiators(rest, initiators);
        
        
    }

    private List<String> registerInitiators(DateraRestClient rest) {
        Map<String,String> initiatorsMap = new HashMap<String,String>();
        //name -- Must be a combination of letters, numbers, spaces, underscores, and dashes between 3 and 128 characters; must start with either a letter or underscore"
        initiatorsMap.put(DateraUtil.constructInitiatorLabel(UUID.randomUUID().toString()),DateraCommon.INITIATOR_1);
        initiatorsMap.put(DateraUtil.constructInitiatorLabel(UUID.randomUUID().toString()),DateraCommon.INITIATOR_2);
        initiatorsMap.put(DateraUtil.constructInitiatorLabel(UUID.randomUUID().toString()),DateraCommon.INITIATOR_3);
        initiatorsMap.put(DateraUtil.constructInitiatorLabel(UUID.randomUUID().toString()),DateraCommon.INITIATOR_4);
        initiatorsMap.put(DateraUtil.constructInitiatorLabel(UUID.randomUUID().toString()),DateraCommon.INITIATOR_5);
        rest.registerInitiators(initiatorsMap);
        List<String> initiators = rest.getInitiators();
        assertTrue(initiators.contains(DateraCommon.INITIATOR_1));
        assertTrue(initiators.contains(DateraCommon.INITIATOR_2));
        assertTrue(initiators.contains(DateraCommon.INITIATOR_3));
        assertTrue(initiators.contains(DateraCommon.INITIATOR_4));
        assertTrue(initiators.contains(DateraCommon.INITIATOR_5));
        
        initiators.clear();
        for(Map.Entry<String, String> iter : initiatorsMap.entrySet())
        {
            initiators.add(iter.getValue());
        }
            
        
        return initiators;
    }
    
    private void unRegisterInitiators(DateraRestClient rest, List<String> initiators)
    {
        for(String iter : initiators)
        {
            rest.unregisterInitiator(iter);
        }
    }

    private void deleteAppInstance(String appInstanceName, DateraRestClient rest) {
        assertTrue(rest.setAdminState(appInstanceName, false));
        assertTrue(rest.deleteAppInstance(appInstanceName));
    }


}
