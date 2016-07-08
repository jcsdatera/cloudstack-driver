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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cloudstack.engine.subsystem.api.storage.CreateCmdResult;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.datastore.lifecycle.DateraPrimaryDataStoreLifeCycle;
import org.apache.cloudstack.storage.datastore.utils.AppInstanceInfo;
import org.apache.cloudstack.storage.datastore.utils.DateraModel;
import org.apache.cloudstack.storage.datastore.utils.DateraRestClient;
import org.apache.cloudstack.storage.datastore.utils.DateraUtil;
import org.apache.http.client.methods.HttpPost;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.cloud.utils.exception.CloudRuntimeException;


@RunWith(MockitoJUnitRunner.class)
public class DateraRestClientTests {

    private String MANAGEMENT_IP = "172.19.175.170";
    private String USERNAME = "admin";
    private String PASSWORD = "password";
    private int PORT = 7718;
    private String APPINSTNAME = null;
    
    @Before
    public void init() {
    	APPINSTNAME = DateraUtil.generateAppInstanceName("test-appInst", UUID.randomUUID().toString());
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createAppInstance(APPINSTNAME));
    	assertTrue(client.createStorageInstance(APPINSTNAME, DateraModel.defaultStorageName, "default"));
    }
    
    @After
    public void cleanUp() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.setAdminState(APPINSTNAME, false));
    	assertTrue(client.deleteAppInstance(APPINSTNAME));
    }

    @Test
    public void testCreateVolume(){
    	
        DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-1", 1, 3));
    }

    @Test
    public void testDeleteVolume(){

    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-2", 2, 2));
        assertTrue(client.setAdminState(APPINSTNAME, false));
        assertTrue(client.deleteVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-2"));
        assertTrue(client.setAdminState(APPINSTNAME, true));
    }

    @Test
    public void testCreateAppInstance()
    {
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	DateraRestClient rest = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        assertTrue(rest.createAppInstance(appInstanceName)); 
        assertTrue(rest.setAdminState(appInstanceName, false));
        assertTrue(rest.deleteAppInstance(appInstanceName));
    }

    @Test
    public void testRegisterInitiatorsOnDatera(){
    	
    	List<String> initiators = new ArrayList<String>();
        String iqn = "iqn.1994-05.com.xenserver:dc785c10806";
        initiators.add(iqn);

        String groupName = "test-init-group_" + APPINSTNAME;
        DateraRestClient rest = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);

        rest.registerInitiators(initiators);
        rest.createInitiatorGroup(groupName, initiators);
        
        List<String> initiatorGroups = new ArrayList<String>();
        initiatorGroups.add(groupName);

        rest.updateStorageWithInitiator(APPINSTNAME, DateraModel.defaultStorageName, null, initiatorGroups);

        initiators = rest.getInitiators();
        rest.unregisterInitiator(iqn);
    }

    @Test
    public void testIsAppInstanceExists(){
    	
    	DateraRestClient rest = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	String appInst1 = DateraUtil.generateAppInstanceName("test-appInst", UUID.randomUUID().toString());
    	assertFalse(rest.isAppInstanceExists(appInst1));

    	String appInst2 = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	rest.createAppInstance(appInst2);
    	assertTrue(rest.isAppInstanceExists(appInst2));
    	assertTrue(rest.setAdminState(appInst2, false));
    	assertTrue(rest.deleteAppInstance(appInst2));
    }
 
    @Test
    public void testGetStorageInfo(){
    	
        DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        assertTrue(client.createStorageInstance(APPINSTNAME, "storage-2", "default"));
        assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "volume-1", 1, 2));
        AppInstanceInfo.StorageInstance info = client.getStorageInfo(APPINSTNAME, "storage-2");
        assertTrue(info.name.equals("storage-2"));
    }

    @Test
    public void testCreateInitiatorGroup() {
    	
    	DateraRestClient rest = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	
		List<String> initiators = new ArrayList<String>();
		
		initiators.add("iqn.2005-03.org.open-iscsi:01cbe94a11");
		initiators.add("iqn.1994-05.com.xenserver:dc785c10806");

        List<String> inits = rest.registerInitiators(initiators);
        //System.out.println(" LISt of oinits " + rest.getInitiators());
        inits = rest.getInitiators();
        assertTrue(inits.contains("iqn.2005-03.org.open-iscsi:01cbe94a11") && inits.contains("iqn.1994-05.com.xenserver:dc785c10806"));
        String initGroupName = "test_initgroup_" + UUID.randomUUID().toString();
        assertTrue(rest.createInitiatorGroup(initGroupName, initiators));
        
        for(String iqn : initiators) {
        	assertTrue(rest.unregisterInitiator(iqn));
        }
        assertTrue(rest.deleteInitiatorGroup(initGroupName));
    }
    
    @Test
    public void testDeleteAppInstanceOnline() {
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        assertTrue(client.createAppInstance(appInstanceName));
        assertFalse(client.deleteAppInstance(appInstanceName));
        assertTrue(client.setAdminState(appInstanceName, false));
        assertTrue(client.deleteAppInstance(appInstanceName));
    }
    
    @Test
    public void testEnumerateAppInstance() {
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createAppInstance(appInstanceName));
    	List <String> resp = client.enumerateAppInstances();
    	assertTrue(resp.contains(appInstanceName));
    	assertTrue(client.setAdminState(appInstanceName, false));
    	assertTrue(client.deleteAppInstance(appInstanceName));
    }
    
    @Test
    public void testCreateStorageInstance() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createStorageInstance(APPINSTNAME, "storage-3", "default"));
    }
    
    @Test
    public void testDeleteVolumeWithAppInstOnline() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-2", 2, 2));
        assertFalse(client.deleteVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-2"));
    }
    
    @Test
    public void testGetVolume() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-4", 1, 2));
    	AppInstanceInfo.VolumeInfo resp = client.getVolumeInfo(APPINSTNAME, DateraModel.defaultStorageName, "vol-4");
    	assertTrue(resp.name.equals("vol-4"));
    }
    
    @Test
    public void testGenerateVolumePayload() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
        List<String> initiators = new ArrayList<String>();
        
        List<String> initiatorGroup = new ArrayList<String>();
		
		initiators.add("iqn.2005-03.org.open-iscsi:01cbe94a11");
		initiators.add("iqn.1994-05.com.xenserver:dc785c10806");
		
    	String resp = client.generateVolumePayload(APPINSTNAME, initiators, initiatorGroup, 1, 2, "allow_all", "default");
    	assertTrue(resp.contains(APPINSTNAME) && resp.contains("iqn.1994-05.com.xenserver:dc785c10806"));
    }
    
    @Test
    public void testResizeVolume() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-6", 1, 2));
    	assertTrue(client.setAdminState(APPINSTNAME, false));
    	assertTrue(client.resizeVolume(APPINSTNAME, DateraModel.defaultStorageName, "vol-6", 2));
    }
    
    @Test
    public void testDeleteInitiatorGroup() {
    	
    	DateraRestClient rest = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	
		List<String> initiators = new ArrayList<String>();
		
		String initGroupName1 = "test_initgroup_" + UUID.randomUUID().toString();
        assertTrue(rest.createInitiatorGroup(initGroupName1, initiators));
        assertTrue(rest.deleteInitiatorGroup(initGroupName1));
        
        // Negative test for deleting a group which does not exists
        assertFalse(rest.deleteInitiatorGroup("test_initgroup_" + UUID.randomUUID().toString()));
    }
    
    @Test
    public void testEnumerateInitiatorNames() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	String iqn = "iqn.2005-03.org.open-iscsi:01cbe94a19";
    	assertTrue(client.registerInitiator("test_initiator_" + UUID.randomUUID().toString(), iqn));
    	List<String> initiators = client.enumerateInitiatorNames();
    	assertTrue(initiators.contains(iqn));
    }
    
    @Test
    public void testUpdateStorageWithInitiator () {
    	
    	List<String> initiators = new ArrayList<String>();
        String iqn = "iqn.1994-05.com.xenserver:dc785c10306";
        initiators.add(iqn);
        
        String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
        
        String groupName = "test-init-group_" + appInstanceName;
        DateraRestClient rest = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);

        rest.registerInitiators(initiators);
        rest.createInitiatorGroup(groupName, initiators);
        
        List<String> initiatorGroups = new ArrayList<String>();
        initiatorGroups.add(groupName);

        boolean bool = rest.updateStorageWithInitiator(appInstanceName, DateraModel.defaultStorageName, null, initiatorGroups);

        initiators = rest.getInitiators();
        rest.unregisterInitiator(iqn);
    }
    
    @Test(expected=CloudRuntimeException.class)
    public void testSetQos() {
    	
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createStorageInstance(APPINSTNAME, "storage-3", "default"));
    	assertTrue(client.createVolume(APPINSTNAME, "storage-3", "volume-8", 1, 2));
    	assertTrue(client.setQos(APPINSTNAME, "storage-3", "volume-8", 1100000L));
    	
    	// Invalid total IOPS
    	assertFalse(client.setQos(APPINSTNAME, "storage-3", "volume-8", 10000000000L));
    }
    
    @Test
    public void testGetVolumes() {
    	
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createStorageInstance(APPINSTNAME, "storage-3", "default"));
    	assertTrue(client.createVolume(APPINSTNAME, "storage-3", "volume-8", 1, 2));
    	List<AppInstanceInfo.VolumeInfo> volumes = client.getVolumes(APPINSTNAME, "storage-3");
    	boolean found = false;
    	for (AppInstanceInfo.VolumeInfo volume : volumes) {
    		if (volume.name.equals("volume-8"))
    			found = true;
    	}
    	assertTrue(found);
    }
    
    @Test
    public void testCreateVolume2() {
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	AppInstanceInfo appInfo = client.createVolume(appInstanceName, null, null, 8, 2, "allow_all", "default");
    	assertEquals(appInfo.storageInstances.storage1.volumes.volume1.size, 8);
    	assertTrue(client.setAdminState(appInstanceName, false));
    	assertTrue(client.deleteAppInstance(appInstanceName));
    }
    
    @Test
    public void testRegisterInitiatorsWithLabel() {
    	
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	Map <String, String> initiators = new HashMap<String, String> ();
    	initiators.put("host1", DateraCommon.INITIATOR_1);
    	List<String> inits = client.registerInitiators(initiators);
    	inits = client.getInitiators();
    	assertTrue(inits.contains(DateraCommon.INITIATOR_1));
    	assertTrue(client.unregisterInitiator(DateraCommon.INITIATOR_1));
    }
    
    @Test
    public void testGenerateNextVolumeName() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	assertTrue(client.createAppInstance(appInstanceName));
    	assertTrue(client.createStorageInstance(appInstanceName, DateraModel.defaultStorageName, "default"));
    	assertTrue(client.createVolume(appInstanceName, DateraModel.defaultStorageName, DateraModel.defaultVolumeName, 1, 2));
    	AppInstanceInfo.VolumeInfo volume1 = client.getVolumeInfo(appInstanceName, DateraModel.defaultStorageName, DateraModel.defaultVolumeName);
    	List<AppInstanceInfo.VolumeInfo> volumes = new ArrayList<AppInstanceInfo.VolumeInfo> ();
    	volumes.add(volume1);
    	assertEquals(client.generateNextVolumeName(volumes, "volume"), "volume-2");
    	assertTrue(client.setAdminState(appInstanceName, false));
    	assertTrue(client.deleteAppInstance(appInstanceName));
    }
    
    @Test
    public void testCreateNextVolume() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	assertTrue(client.createAppInstance(appInstanceName));
    	assertTrue(client.createStorageInstance(appInstanceName, DateraModel.defaultStorageName, "default"));
    	assertTrue(client.createVolume(appInstanceName, DateraModel.defaultStorageName, DateraModel.defaultVolumeName, 1, 2));
    	assertEquals(client.createNextVolume(appInstanceName, DateraModel.defaultStorageName, 3), "volume-2");
    	assertTrue(client.setAdminState(appInstanceName, false));
    	assertTrue(client.deleteAppInstance(appInstanceName));
    }
    
    @Test
    public void testUpdateQos() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	assertTrue(client.createVolume(APPINSTNAME, DateraModel.defaultStorageName, "volume-12", 2, 2));
    	assertTrue(client.setQos(APPINSTNAME, DateraModel.defaultStorageName, "volume-12", 1100000L));
    	assertTrue(client.updateQos(APPINSTNAME, DateraModel.defaultStorageName, "volume-12", 1100010L));
    }
    
    @Test
    public void testGetAppInstance() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	assertTrue(client.createAppInstance(appInstanceName));
    	DateraModel.AppModel appInst = client.getAppInstanceInfo(appInstanceName);
    	assertTrue(appInst.name.equals(appInstanceName));
    	assertTrue(client.setAdminState(appInstanceName, false));
    	assertTrue(client.deleteAppInstance(appInstanceName));
    }
    
    @Test
    public void testGetQos() {
    	DateraRestClient client = new DateraRestClient(MANAGEMENT_IP, PORT, USERNAME, PASSWORD);
    	String appInstanceName = DateraUtil.generateAppInstanceName("test-appInst-", UUID.randomUUID().toString());
    	assertTrue(client.createAppInstance(appInstanceName));
    	assertTrue(client.createStorageInstance(appInstanceName, DateraModel.defaultStorageName, "default"));
    	assertTrue(client.createVolume(appInstanceName, DateraModel.defaultStorageName, "volume-12", 1, 3));
    	
    	assertTrue(client.setQos(appInstanceName, DateraModel.defaultStorageName, "volume-12", 100000L));
    	DateraModel.PerformancePolicy  policy = client.getQos(appInstanceName, DateraModel.defaultStorageName, "volume-12");
    	assertTrue(policy.totalIopsMax == 100000L);
    	assertTrue(client.setAdminState(appInstanceName, false));
    	assertTrue(client.deleteAppInstance(appInstanceName));
    }

}

