/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cloudstack.storage.datastore.utils.AppInstanceInfo;
import org.apache.cloudstack.storage.datastore.utils.AppInstanceInfo.StorageInstance;
import org.apache.cloudstack.storage.datastore.utils.DateraModel;
import org.apache.cloudstack.storage.datastore.utils.DateraRestClient;
import org.apache.cloudstack.storage.datastore.utils.DateraUtil;
import org.apache.cloudstack.storage.datastore.utils.DateraRestClientMgr;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.cloud.utils.exception.CloudRuntimeException;

@RunWith(MockitoJUnitRunner.class)
public class DateraRestMgrMockTest {
    
    private DateraRestClient rest = null;
    private DateraRestClientMgr restMgr = null;
    private DateraUtil.DateraMetaData dtMetaData = null;
    private String appName = "test_global_app_inst_1";
    
    @Before
    public void init() {
        rest  = mock(DateraRestClient.class);
        restMgr = DateraRestClientMgr.getInstance();
        dtMetaData = new DateraUtil.DateraMetaData(DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, 
                DateraCommon.PASSWORD, null, DateraCommon.DEFAULT_REPLICA, 
                DateraCommon.DEFAULT_NETWORK_POOL_NAME, appName, DateraModel.defaultStorageName, null,null);
    }

    @Test
    public void testCreateVolume() {
        final String appName = "test_app_inst_1";
        when(rest.isAppInstanceExists(appName)).thenReturn(false);
        List<String> netPool = new ArrayList();
        netPool.add("default");
        when(rest.enumerateNetworkPool()).thenReturn(netPool);

        int dtVolSize = DateraUtil.getVolumeSizeInGB(DateraCommon.DEFAULT_CAPACITY_BYTES);
        when(rest.createVolume(appName, null, null, dtVolSize, DateraCommon.DEFAULT_REPLICA, 
                DateraRestClient.ACCESS_CONTROL_MODE_ALLOW_ALL, DateraCommon.DEFAULT_NETWORK_POOL_NAME)).thenReturn(new AppInstanceInfo());

        when(rest.setQos(appName, DateraModel.defaultStorageName, DateraModel.defaultVolumeName, DateraCommon.DEFAULT_CAPACITY_IOPS)).thenReturn(true);
        
        /*AppInstanceInfo.VolumeInfo volInfo = new AppInstanceInfo().new VolumeInfo();
        volInfo.name = DateraModel.defaultVolumeName;
        volInfo.opState = "available";*/
        AppInstanceInfo.StorageInstance storageInfo = new AppInstanceInfo().new StorageInstance();
        storageInfo.volumes = new AppInstanceInfo().new VolumeInstances();
        storageInfo.volumes.volume1 = new AppInstanceInfo().new VolumeInfo();

        storageInfo.name = DateraModel.defaultStorageName;
        storageInfo.opState = DateraRestClient.OP_STATE_AVAILABLE;
        storageInfo.volumes.volume1.name = DateraModel.defaultVolumeName;
        storageInfo.volumes.volume1.opState = DateraRestClient.OP_STATE_AVAILABLE;

        //when(rest.getVolumeInfo(appName, DateraModel.defaultStorageName, DateraModel.defaultVolumeName)).thenReturn(volInfo);
        when(rest.getStorageInfo(appName, DateraModel.defaultStorageName)).thenReturn(storageInfo);

        when(rest.setAdminState(appName, false)).thenReturn(true);
        when(rest.deleteAppInstance(appName)).thenReturn(true);
        //when(rest.getStorageInfo(appName, DateraModel.defaultStorageName)).thenReturn(null);

        restMgr.createVolume(rest, DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME,
                DateraCommon.PASSWORD, appName, DateraCommon.DEFAULT_NETWORK_POOL_NAME, DateraCommon.DEFAULT_CAPACITY_BYTES, 
                DateraCommon.DEFAULT_REPLICA, DateraCommon.DEFAULT_CAPACITY_IOPS);
        storageInfo.volumes.volume1.opState = "unavailable";
        try {
        restMgr.createVolume(rest, DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME,
                DateraCommon.PASSWORD, appName, DateraCommon.DEFAULT_NETWORK_POOL_NAME, DateraCommon.DEFAULT_CAPACITY_BYTES, 
                DateraCommon.DEFAULT_REPLICA, DateraCommon.DEFAULT_CAPACITY_IOPS);
        } catch(Exception ex) {
             assertEquals(CloudRuntimeException.class, ex.getClass());
        }
    }
    
    @Test
    public void testRegisterInitiators() {
        
        final String appName = "test_app_inst_2";
        
        Map<String, String> initiators = new HashMap<String, String>();
        initiators.put("host1", DateraCommon.INITIATOR_1);

        List<String> regInitList = new ArrayList<String>(initiators.values());

        when(rest.registerInitiators(initiators)).thenReturn(regInitList);
        when(rest.createInitiatorGroup("test_init_grp_1", regInitList)).thenReturn(true);
        
        AppInstanceInfo.StorageInstance storageInfo = new AppInstanceInfo().new StorageInstance();
        storageInfo.volumes = new AppInstanceInfo().new VolumeInstances();
        storageInfo.volumes.volume1 = new AppInstanceInfo().new VolumeInfo();

        storageInfo.name = DateraModel.defaultStorageName;
        storageInfo.opState = DateraRestClient.OP_STATE_AVAILABLE;
        storageInfo.volumes.volume1.name = DateraModel.defaultVolumeName;
        storageInfo.volumes.volume1.opState = DateraRestClient.OP_STATE_AVAILABLE;

        when(rest.getStorageInfo(appName, DateraModel.defaultStorageName)).thenReturn(storageInfo);

        List<String> initGroups = new ArrayList <String>();
        initGroups.add("test_init_grp_1");
        when(rest.updateStorageWithInitiator(appName, DateraModel.defaultStorageName, null, initGroups)).thenReturn(true);

        assertTrue(restMgr.registerInitiatorsAndUpdateStorageWithInitiatorGroup(rest, DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, 
                DateraCommon.PASSWORD, appName, DateraModel.defaultStorageName, "test_init_grp_1", initiators, DateraUtil.PRIMARY_STORAGE_CREATION_TIMEOUT));
        storageInfo.volumes.volume1.opState = "unavailable";
        assertFalse(restMgr.registerInitiatorsAndUpdateStorageWithInitiatorGroup(rest, DateraCommon.MANAGEMENT_IP, DateraCommon.PORT, DateraCommon.USERNAME, 
                DateraCommon.PASSWORD, appName, DateraModel.defaultStorageName, "test_init_grp_1", initiators, DateraUtil.PRIMARY_STORAGE_CREATION_TIMEOUT));
    }
    
    @Test
    public void testUpdatePrimaryStorageCapacityBytes() {
        
        int dtVolumeSize = restMgr.getDateraCompatibleVolumeInGB(DateraCommon.DEFAULT_CAPACITY_BYTES);
        when(rest.setAdminState(dtMetaData.appInstanceName, false)).thenReturn(true);
        when(rest.resizeVolume(dtMetaData.appInstanceName, dtMetaData.storageInstanceName, DateraModel.defaultVolumeName, dtVolumeSize)).thenReturn(true);
        when(rest.setAdminState(dtMetaData.appInstanceName, true)).thenReturn(true);
        assertTrue(restMgr.updatePrimaryStorageCapacityBytes(rest, dtMetaData, DateraCommon.DEFAULT_CAPACITY_BYTES));
    }
    
    @Test
    public void testUpdatePrimaryStorageIOPS() {
        
        when(rest.updateQos(dtMetaData.appInstanceName, dtMetaData.storageInstanceName,
                DateraModel.defaultVolumeName, DateraCommon.DEFAULT_CAPACITY_IOPS)).thenReturn(true);
        assertTrue(restMgr.updatePrimaryStorageIOPS(rest, dtMetaData, DateraCommon.DEFAULT_CAPACITY_IOPS));
    }
    
    @Test
    public void testGetStorageInfo() {
        when(rest.getStorageInfo(dtMetaData.appInstanceName, dtMetaData.storageInstanceName)).thenReturn(new AppInstanceInfo().new StorageInstance());
        assertTrue(restMgr.getStorageInfo(rest, dtMetaData) instanceof AppInstanceInfo.StorageInstance);
    }
    
    @Test
    public void testGetDateraCompatibleVolumeInGB() {
        assertEquals(restMgr.getDateraCompatibleVolumeInGB(10000000000L), 9);
    }
    
    @Test
    public void testGetCloudstackCompatibleVolumeSize() {
        assertEquals(restMgr.getCloudstackCompatibleVolumeSize(5), 5368709120L);
    }
    
    @Test
    public void testdDeleteInitiatorGroup() {
        when(rest.deleteInitiatorGroup(dtMetaData.initiatorGroupName)).thenReturn(true);
        assertTrue(restMgr.deleteInitiatorGroup(rest, dtMetaData));
    }
    
    @Test
    public void testDeleteAppInstance() {
        when(rest.setAdminState(dtMetaData.appInstanceName, false)).thenReturn(true);
        when(rest.deleteAppInstance(dtMetaData.appInstanceName)).thenReturn(true);
        assertTrue(restMgr.deleteAppInstance(rest, dtMetaData));
    }

}