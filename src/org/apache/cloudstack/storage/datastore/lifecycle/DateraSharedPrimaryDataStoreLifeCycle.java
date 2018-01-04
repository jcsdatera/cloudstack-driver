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

package org.apache.cloudstack.storage.datastore.lifecycle;

import com.cloud.agent.api.StoragePoolInfo;
import com.cloud.capacity.CapacityManager;
import com.cloud.dc.ClusterDetailsDao;
import com.cloud.dc.ClusterVO;
import com.cloud.dc.dao.ClusterDao;
import com.cloud.dc.dao.DataCenterDao;
import com.cloud.host.Host;
// import com.cloud.host.Host.Type;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.resource.ResourceManager;
//import com.cloud.storage.SnapshotVO;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StoragePool;
import com.cloud.storage.StoragePoolAutomation;
import com.cloud.storage.StoragePoolHostVO;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
//import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.StoragePoolHostDao;
import com.cloud.utils.StringUtils;
import com.cloud.utils.db.GlobalLock;
import com.cloud.utils.exception.CloudRuntimeException;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.cloudstack.engine.subsystem.api.storage.ClusterScope;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.HostScope;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreLifeCycle;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreParameters;
import org.apache.cloudstack.engine.subsystem.api.storage.ZoneScope;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
//import org.apache.cloudstack.storage.datastore.util.DateraObject.AppInstance;
//import org.apache.cloudstack.storage.datastore.util.DateraObject.DateraConnection;
//import org.apache.cloudstack.storage.datastore.util.DateraObject.DateraError;
import org.apache.cloudstack.storage.datastore.util.DateraUtil;
import org.apache.cloudstack.storage.datastore.util.DateraObject;
import org.apache.cloudstack.storage.volume.datastore.PrimaryDataStoreHelper;
import org.apache.log4j.Logger;

public class DateraSharedPrimaryDataStoreLifeCycle implements PrimaryDataStoreLifeCycle {
    private static final Logger s_logger = Logger.getLogger(DateraSharedPrimaryDataStoreLifeCycle.class);
    private static final int s_lockTimeInSeconds = 300;
    @Inject
    private CapacityManager _capacityMgr;
    @Inject
    private DataCenterDao zoneDao;
    @Inject
    private ClusterDao _clusterDao;
    @Inject
    private ClusterDetailsDao _clusterDetailsDao;
    @Inject
    private PrimaryDataStoreDao storagePoolDao;
    @Inject
    private StoragePoolDetailsDao _storagePoolDetailsDao;
    @Inject
    private HostDao _hostDao;
    @Inject
    private PrimaryDataStoreHelper dataStoreHelper;
    @Inject
    private ResourceManager _resourceMgr;
    @Inject
    private SnapshotDao _snapshotDao;
    @Inject
    private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject
    private StorageManager _storageMgr;
    @Inject
    private StoragePoolHostDao _storagePoolHostDao;
    @Inject
    private StoragePoolAutomation storagePoolAutomation;

    @Override
    public DataStore initialize(Map<String, Object> dsInfos)
    {
        String url = (String)dsInfos.get("url");
        Long zoneId = (Long)dsInfos.get("zoneId");
        Long podId = (Long)dsInfos.get("podId");
        Long clusterId = (Long)dsInfos.get("clusterId");
        String storagePoolName = (String)dsInfos.get("name");
        String providerName = (String)dsInfos.get("providerName");
        Long capacityBytes = (Long)dsInfos.get("capacityBytes");
        Long capacityIops = (Long)dsInfos.get("capacityIops");
        String tags = (String)dsInfos.get("tags");

        Map<String, String> details = (Map)dsInfos.get("details");

        String storageVip = DateraUtil.getStorageVip(url);

        int storagePort = DateraUtil.getStoragePort(url);
        int numReplicas = DateraUtil.getNumReplicas(url);
        String volPlacement = DateraUtil.getVolPlacement(url);
        String clusterAdminUsername = DateraUtil.getValue("clusterAdminUsername", url);
        String clusterAdminPassword = DateraUtil.getValue("clusterAdminPassword", url);

        PrimaryDataStoreParameters parameters = new PrimaryDataStoreParameters();
        HypervisorType hypervisorType=null;
        String uuid=null;
        s_logger.debug("Datera - Loading Datera Shared Primary Data Store Driver :" + DateraUtil.DRIVER_VERSION);
        // Only cluster-wide is supported in shared mode
        if (clusterId != null)
        {
            if (podId == null) {
                throw new CloudRuntimeException("The Pod ID must be specified.");
            }
            if (zoneId == null) {
                throw new CloudRuntimeException("The Zone ID must be specified.");
            }
            ClusterVO cluster = (ClusterVO)this._clusterDao.findById(clusterId);
            uuid = "DateraShared_" + clusterId + "_" + storageVip + "_" + clusterAdminUsername + "_" + numReplicas + "_" + volPlacement;
            s_logger.debug("Setting Datera cluster-wide primary storage uuid to " + uuid);
            parameters.setPodId(podId);
            parameters.setClusterId(clusterId);

            hypervisorType = getHypervisorTypeForCluster(clusterId.longValue());
            if (!isSupportedHypervisorType(hypervisorType)) {
                throw new CloudRuntimeException(hypervisorType + " is not a supported hypervisor type.");
            }
        }
        else
        {
            s_logger.debug("Datera - clusterId is null");
            throw new CloudRuntimeException("Zone wide scope is not supported in Datera shared primary storage driver");
        }

        String datacenter = DateraUtil.getValue("datacenter", url, false);
        if ((HypervisorType.VMware.equals(hypervisorType)) && (datacenter == null)) {
            throw new CloudRuntimeException("'Datacenter' must be set in URL for hypervisor type of " + HypervisorType.VMware);
        }
        if ((capacityBytes == null) || (capacityBytes.longValue() <= 0L)) {
            throw new IllegalArgumentException("'capacityBytes' must be present and greater than 0.");
        }
        if ((capacityIops == null) || (capacityIops.longValue() <= 0L)) {
            throw new IllegalArgumentException("'capacityIops' must be present and greater than 0.");
        }
        parameters.setHost(storageVip);
        parameters.setPort(storagePort);
        parameters.setPath(DateraUtil.getModifiedUrl(url));
        parameters.setType(StoragePoolType.Iscsi);
        parameters.setUuid(uuid);
        parameters.setZoneId(zoneId);
        parameters.setName(storagePoolName);
        parameters.setProviderName(providerName);
        parameters.setManaged(true);
        parameters.setCapacityBytes(capacityBytes.longValue());
        parameters.setUsedBytes(0L);
        parameters.setCapacityIops(capacityIops);
        parameters.setHypervisorType(HypervisorType.Any);
        parameters.setTags(tags);
        parameters.setDetails(details);

        String managementVip = DateraUtil.getManagementVip(url);
        int managementPort = DateraUtil.getManagementPort(url);

        details.put("mVip", managementVip);
        details.put("mPort", String.valueOf(managementPort));

        details.put("clusterAdminUsername", clusterAdminUsername);
        details.put("clusterAdminPassword", clusterAdminPassword);

        long lClusterDefaultMinIops = 100L;
        long lClusterDefaultMaxIops = 15000L;
        try
        {
            String clusterDefaultMinIops = DateraUtil.getValue("clusterDefaultMinIops", url);
            if ((clusterDefaultMinIops != null) && (clusterDefaultMinIops.trim().length() > 0)) {
                lClusterDefaultMinIops = Long.parseLong(clusterDefaultMinIops);
            }
        }
        catch (NumberFormatException ex)
        {
            s_logger.warn("Cannot parse the setting of clusterDefaultMinIops, using default value: " + lClusterDefaultMinIops + ". Exception: " + ex);
        }
        try
        {
            String clusterDefaultMaxIops = DateraUtil.getValue("clusterDefaultMaxIops", url);
            if ((clusterDefaultMaxIops != null) && (clusterDefaultMaxIops.trim().length() > 0)) {
                lClusterDefaultMaxIops = Long.parseLong(clusterDefaultMaxIops);
            }
        }
        catch (NumberFormatException ex)
        {
            s_logger.warn("Cannot parse the setting of clusterDefaultMaxIops, using default value: " + lClusterDefaultMaxIops + ". Exception: " + ex);
        }
        if (lClusterDefaultMinIops > lClusterDefaultMaxIops) {
            throw new CloudRuntimeException("The parameter 'clusterDefaultMinIops' must be less than or equal to the parameter 'clusterDefaultMaxIops'.");
        }
        Long volSizeBytes = capacityBytes;
        Long maxIops = capacityIops;

        details.put("clusterDefaultMinIops", String.valueOf(lClusterDefaultMinIops));
        details.put("clusterDefaultMaxIops", String.valueOf(lClusterDefaultMaxIops));
        details.put("numReplicas", String.valueOf(numReplicas));
        details.put("volPlacement", volPlacement);

        // this adds a row in the db table : cloud.storage_pool table for each primary storage
        DataStore dataStore = this.dataStoreHelper.createPrimaryDataStore(parameters);

        long storagePoolId = dataStore.getId();
        Preconditions.checkNotNull(Long.valueOf(storagePoolId), "storagePoolId should not be null");

        String iqn = "";
        try
        {
            s_logger.info("Datera - creating shared volume");
            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, this._storagePoolDetailsDao);

            DateraObject.AppInstance appInstance = createVolume(conn, dataStore, volSizeBytes.longValue(), maxIops.longValue());

            Preconditions.checkNotNull(appInstance);
            s_logger.info("Datera - shared volume created ");

            iqn = appInstance.getIqn();
            Preconditions.checkNotNull(iqn);
        }
        catch (Exception ex)
        {
            this.storagePoolDao.expunge(Long.valueOf(dataStore.getId()));
            s_logger.error("Datera - creating shared volume failed");
            throw new CloudRuntimeException(ex.getMessage());
        }
        if (HypervisorType.VMware.equals(hypervisorType))
        {
            String datastore = iqn.replace("/", "_");
            String path = "/" + datacenter + "/" + datastore;

            parameters.setHost("VMFS datastore: " + path);
            parameters.setPort(0);
            parameters.setPath(path);

            details.put(DateraUtil.DATASTORE_NAME, datastore);
            details.put(DateraUtil.IQN, iqn);
            details.put(DateraUtil.STORAGE_VIP, storageVip);
            details.put(DateraUtil.STORAGE_PORT, String.valueOf(storagePort));
        }
        else
        {
            parameters.setHost(storageVip);
            parameters.setPort(storagePort);
            parameters.setPath(iqn);
        }
        return dataStore;
    }

    @Override
    public boolean attachHost(DataStore store, HostScope scope, StoragePoolInfo existingInfo)
    {
        return true;
    }

    @Override
    public boolean attachCluster(DataStore datastore, ClusterScope scope)
    {
        s_logger.info("Datera - DateraSharedPrimaryDataStoreDriver.attachCluster() is called");

        PrimaryDataStoreInfo primaryDataStoreInfo = (PrimaryDataStoreInfo)datastore;

        List<HostVO> allHosts = this._resourceMgr.listAllUpAndEnabledHosts(Host.Type.Routing, primaryDataStoreInfo.getClusterId(), primaryDataStoreInfo
                .getPodId(), primaryDataStoreInfo.getDataCenterId());
        if (allHosts.isEmpty())
        {
            // TODO: Delete Datera appInstance
            this.storagePoolDao.expunge(Long.valueOf(primaryDataStoreInfo.getId()));

            throw new CloudRuntimeException("No host up to associate a storage pool within the cluster " + primaryDataStoreInfo.getClusterId());
        }
        List<HostVO> poolHosts = new ArrayList();
        for (HostVO host : allHosts) {
            try
            {
                this._storageMgr.connectHostToSharedPool(host.getId(), primaryDataStoreInfo.getId());

                poolHosts.add(host);
            }
            catch (Exception e)
            {
                s_logger.warn("Unable to establish a connection between " + host + " and " + primaryDataStoreInfo, e);
            }
        }
        if (poolHosts.isEmpty())
        {
            s_logger.warn("No host can access storage pool '" + primaryDataStoreInfo + "' on cluster '" + primaryDataStoreInfo.getClusterId() + "'.");
            // TODO: Delete Datera appInstance

            this.storagePoolDao.expunge(Long.valueOf(primaryDataStoreInfo.getId()));

            throw new CloudRuntimeException("Failed to access storage pool");
        }
        this.dataStoreHelper.attachCluster(datastore);

        return true;
    }

    @Override
    public boolean attachZone(DataStore dataStore, ZoneScope scope, HypervisorType hypervisorType) {

        throw new CloudRuntimeException("Zone wide scope is not supported in Datera shared primary storage driver");
    }

    @Override
    public boolean maintain(DataStore dataStore)
    {
        this.storagePoolAutomation.maintain(dataStore);
        this.dataStoreHelper.maintain(dataStore);

        return true;
    }

    @Override
    public boolean cancelMaintain(DataStore store)
    {
        this.dataStoreHelper.cancelMaintain(store);
        this.storagePoolAutomation.cancelMaintain(store);

        return true;
    }

    @Override
    public boolean deleteDataStore(DataStore dataStore)
    {
        s_logger.debug("Datera - DateraSharedPrimaryDataStoreLifeCycle.deleteDataStore() called");

        List<StoragePoolHostVO> hostPoolRecords = _storagePoolHostDao.listByPoolId(dataStore.getId());

        HypervisorType hypervisorType = null;

        if (hostPoolRecords.size() > 0 ) {
            hypervisorType = getHypervisorType(hostPoolRecords.get(0).getHostId());
        }

        if (!isSupportedHypervisorType(hypervisorType)) {
            throw new CloudRuntimeException(hypervisorType + " is not a supported hypervisor type.");
        }

        StoragePool storagePool = (StoragePool)dataStore;
        StoragePoolVO storagePoolVO = _primaryDataStoreDao.findById(storagePool.getId());
        List<VMTemplateStoragePoolVO> unusedTemplatesInPool = _tmpltMgr.getUnusedTemplatesInPool(storagePoolVO);

        for (VMTemplateStoragePoolVO templatePoolVO : unusedTemplatesInPool) {
            _tmpltMgr.evictTemplateFromStoragePool(templatePoolVO);
        }

        Long clusterId = null;

        for (StoragePoolHostVO host : hostPoolRecords) {
            DeleteStoragePoolCommand deleteCmd = new DeleteStoragePoolCommand(storagePool);

            if (HypervisorType.VMware.equals(hypervisorType)) {
                deleteCmd.setRemoveDatastore(true);

                Map<String, String> details = new HashMap<String, String>();

                StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePool.getId(), DateraUtil.DATASTORE_NAME);

                details.put(DeleteStoragePoolCommand.DATASTORE_NAME, storagePoolDetail.getValue());

                storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePool.getId(), DateraUtil.IQN);

                details.put(DeleteStoragePoolCommand.IQN, storagePoolDetail.getValue());

                storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePool.getId(), DateraUtil.STORAGE_VIP);

                details.put(DeleteStoragePoolCommand.STORAGE_HOST, storagePoolDetail.getValue());

                storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePool.getId(), DateraUtil.STORAGE_PORT);

                details.put(DeleteStoragePoolCommand.STORAGE_PORT, storagePoolDetail.getValue());

                deleteCmd.setDetails(details);
            }

            final Answer answer = _agentMgr.easySend(host.getHostId(), deleteCmd);

            if (answer != null && answer.getResult()) {
                s_logger.info("Successfully deleted storage pool using Host ID " + host.getHostId());

                HostVO hostVO = _hostDao.findById(host.getHostId());

                if (hostVO != null) {
                    clusterId = hostVO.getClusterId();
                }

                break;
            }
            else {
                s_logger.error("Failed to delete storage pool using Host ID " + host.getHostId() + ": " + answer.getResult());
            }
        }

        List<SnapshotVO> lstSnapshots = this._snapshotDao.listAll();
        if (lstSnapshots != null) {
            for (SnapshotVO snapshot : lstSnapshots) {
                SnapshotDetailsVO snapshotDetails = (SnapshotDetailsVO) this._snapshotDetailsDao.findDetail(snapshot.getId(), "DateraStoragePoolId");
                if ((snapshotDetails != null) && (snapshotDetails.getValue() != null) && (Long.parseLong(snapshotDetails.getValue()) == dataStore.getId())) {
                    throw new CloudRuntimeException("This primary storage cannot be deleted because it currently contains one or more snapshots.");
                }
            }
        }

        if (clusterId != null) {
            //remove initiator from the storage
            //unregister the initiators or remove the initiator group
            ClusterVO cluster = _clusterDao.findById(clusterId);
            GlobalLock lock = GlobalLock.getInternLock(cluster.getUuid());

            if(!lock.lock(DateraUtil.LOCK_TIME_IN_SECOND)){
                String err = DateraUtil.LOG_PREFIX+" Deleteing storage could not lock on : "+cluster.getUuid();
                throw new CloudRuntimeException(err);
            }
            try{

                if(isDateraSupported(hypervisorType)) {
                    // The CloudStack operation must continue even of the Datera Side operation does not succeed
                    deleteVolume(dataStore);
                }
            }
            catch(Exception ex){
                //do not throw any exception here
            }
            finally{
                lock.unlock();
                lock.releaseRef();
            }

        }

        return _primaryDataStoreHelper.deletePrimaryDataStore(dataStore);
    }

    @Override
    public boolean migrateToObjectStore(DataStore store)
    {
        return false;
    }

    @Override
    public void updateStoragePool(StoragePool storagePool, Map<String, String> details)
    {
        StoragePoolVO storagePoolVo = (StoragePoolVO)this.storagePoolDao.findById(Long.valueOf(storagePool.getId()));

        String strCapacityBytes = (String)details.get("capacityBytes");
        Long capacityBytes = strCapacityBytes != null ? Long.valueOf(Long.parseLong(strCapacityBytes)) : null;
        if (capacityBytes != null)
        {
            long usedBytes = this._capacityMgr.getUsedBytes(storagePoolVo);
            if (capacityBytes.longValue() < usedBytes) {
                throw new CloudRuntimeException("Cannot reduce the number of bytes for this storage pool as it would lead to an insufficient number of bytes");
            }
        }
        String strCapacityIops = (String)details.get("capacityIops");
        Long capacityIops = strCapacityIops != null ? Long.valueOf(Long.parseLong(strCapacityIops)) : null;
        if (capacityIops != null)
        {
            long usedIops = this._capacityMgr.getUsedIops(storagePoolVo);
            if (capacityIops.longValue() < usedIops) {
                throw new CloudRuntimeException("Cannot reduce the number of IOPS for this storage pool as it would lead to an insufficient number of IOPS");
            }
        }
    }

    @Override
    public void enableStoragePool(DataStore dataStore)
    {
        this.dataStoreHelper.enable(dataStore);
    }

    @Override
    public void disableStoragePool(DataStore dataStore)
    {
        this.dataStoreHelper.disable(dataStore);
    }

    private HypervisorType getHypervisorTypeForCluster(long clusterId)
    {
        ClusterVO cluster = (ClusterVO)this._clusterDao.findById(Long.valueOf(clusterId));
        if (cluster == null) {
            throw new CloudRuntimeException("Cluster ID '" + clusterId + "' was not found in the database.");
        }
        return cluster.getHypervisorType();
    }

    private static boolean isSupportedHypervisorType(HypervisorType hypervisorType)
    {
        return (HypervisorType.XenServer.equals(hypervisorType)) || (HypervisorType.VMware.equals(hypervisorType));
    }

    private HypervisorType getHypervisorType(long hostId)
    {
        HostVO host = (HostVO)this._hostDao.findById(Long.valueOf(hostId));
        if (host != null) {
            return host.getHypervisorType();
        }
        return HypervisorType.None;
    }

    private DateraObject.AppInstance createVolume(DateraObject.DateraConnection conn, DataStore dataStore, long volSizeBytes, long maxIops)
    {
        String errMsg = null;
        int numReplicas = 0;
        int volMaxIops = 0;

        int volSizeGb = 0;
        String volPlacement = null;
        String appInstanceName = null;

        Long storagePoolId = Long.valueOf(dataStore.getId());

        Preconditions.checkArgument(storagePoolId.longValue() > 0L, "storagePoolId should be > 0");

        s_logger.debug("Datera - DateraSharedPrimaryDataStoreDriver.createVolume() called");
        try
        {
            if (maxIops < 0L)
            {
                volMaxIops = Ints.checkedCast(getDefaultMaxIops(storagePoolId.longValue()));
                s_logger.debug("Datera - maxIops == null || maxIops < 0 ");
            }
            else
            {
                volMaxIops = Ints.checkedCast(maxIops);
            }
            numReplicas = getNumReplicas(storagePoolId.longValue());
            volPlacement = getVolPlacement(storagePoolId.longValue());

            volSizeGb = DateraUtil.bytesToGb(volSizeBytes);
            if (volSizeGb == 0)
            {
                s_logger.error("Datera - Volume size is still 0:");
                return null;
            }
            appInstanceName = getAppInstanceName(dataStore);
            if (volPlacement == null)
            {
                s_logger.debug("Datera - Creating app_instance name: " + appInstanceName + " volSizeGb: " + String.valueOf(volSizeGb) + " maxIops: " +
                        String.valueOf(volMaxIops) + " numReplicas: " + String.valueOf(numReplicas));
                return DateraUtil.createAppInstance(conn, appInstanceName, volSizeGb, volMaxIops, numReplicas);
            }
            s_logger.debug("Datera - Creating app_instance name: " + appInstanceName + " volSizeGb: " + String.valueOf(volSizeGb) + " maxIops: " +
                    String.valueOf(volMaxIops) + " numReplicas: " + String.valueOf(numReplicas) + " volPlacement: " + volPlacement);
            return DateraUtil.createAppInstance(conn, appInstanceName, volSizeGb, volMaxIops, numReplicas, volPlacement);
        }
        catch (UnsupportedEncodingException|DateraObject.DateraError ex)
        {
            s_logger.warn("Datera - Failed to create Datera volume");
            errMsg = ex.getMessage();
            s_logger.error("Datera - Datera API error msg: " + errMsg);
            s_logger.error("Datera - DateraSharedPrimaryDataStoreDriver.createVolume() returns null");
            return null;
        }
        catch (Exception ex)
        {
            s_logger.warn("Datera - Failed to create Datera volume");
            errMsg = ex.getMessage();
            s_logger.error("Datera - error msg: " + errMsg);
            s_logger.error("Datera - DateraSharedPrimaryDataStoreDriver.createVolume() returns null");
        }
        return null;
    }


    private void deleteVolume(DateraObject.DateraConnection conn, DataStore dataStore) {

        Long storagePoolId = dataStore.getId();

        s_logger.debug("Datera - DateraSharedPrimaryDataStoreDriver.deleteVolume() with "+String.valueOf(storagePoolId)+" called");

        if (storagePoolId == null) {
            return; // this volume was never assigned to a storage pool, so no SAN volume should exist for it
        }

        try {
            String appInstanceName = getAppInstanceName(volumeInfo);
            s_logger.debug("Datera - DateraSharedPrimaryDataStoreDriver.deleteVolume(), deleting "+appInstanceName);
            DateraUtil.deleteAppInstance(conn, appInstanceName);

        } catch (UnsupportedEncodingException | DateraObject.DateraError e) {
            String errMesg = "Error deleting app instance for storagePool : " + String.valueOf(storagePoolId);
            s_logger.warn(errMesg, e);
            throw new CloudRuntimeException(errMesg);
        }
    }

    private long getDefaultMaxIops(long storagePoolId)
    {
        StoragePoolDetailVO storagePoolDetail = (StoragePoolDetailVO)this._storagePoolDetailsDao.findDetail(storagePoolId, "clusterDefaultMaxIops");

        String clusterDefaultMaxIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMaxIops);
    }

    private int getNumReplicas(long storagePoolId)
    {
        StoragePoolDetailVO storagePoolDetail = (StoragePoolDetailVO)this._storagePoolDetailsDao.findDetail(storagePoolId, "numReplicas");

        String clusterDefaultReplicas = storagePoolDetail.getValue();

        return Integer.parseInt(clusterDefaultReplicas);
    }

    private String getVolPlacement(long storagePoolId)
    {
        StoragePoolDetailVO storagePoolDetail = (StoragePoolDetailVO)this._storagePoolDetailsDao.findDetail(storagePoolId, "volPlacement");

        String clusterDefaultVolPlacement = storagePoolDetail.getValue();

        return clusterDefaultVolPlacement;
    }

    private String getAppInstanceName(DataStore dataStore)
    {
        PrimaryDataStoreInfo primaryDataStoreInfo = (PrimaryDataStoreInfo)datastore;

        ArrayList<String> name = new ArrayList();

        long clusterId = primaryDataStoreInfo.getClusterId();
        name.add("CS");
        name.add(dataStore.getName().toString());
        name.add(String.valueOf(clusterId));
        name.add(String.valueOf(dataStore.getId()));

        return StringUtils.join("-", name.toArray());
    }

    private boolean grantAccess(DateraObject.DateraConnection conn, Host host, DataStore dataStore)
    {
        s_logger.debug("Datera - DateraSharedPrimaryDataStoreDriver.grantAccess() called");
        Preconditions.checkArgument(conn != null, "'conn' should not be 'null'");
        Preconditions.checkArgument(host != null, "'host' should not be 'null'");
        Preconditions.checkArgument(dataStore != null, "'dataStore' should not be 'null'");

        long storagePoolId = 0L;

        String appInstanceName = null;
        DateraObject.AppInstance appInstance = null;
        long clusterId = 0L;
        ClusterVO cluster = null;
        GlobalLock lock = null;
        boolean result = false;
        try
        {
            storagePoolId = dataStore.getId();
            cluster = (ClusterVO)this._clusterDao.findById(Long.valueOf(clusterId));
            clusterId = host.getClusterId().longValue();

            appInstanceName = getAppInstanceName(dataStore);
            appInstance = DateraUtil.getDateraAppInstance(conn, appInstanceName);

            Preconditions.checkArgument(appInstance != null, "appInstance "+ appInstanceName + " was not found!");


            lock = GlobalLock.getInternLock(cluster.getUuid());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            s_logger.error("Datera - DateraSharedPrimaryDataStoreDriver.grantAccess() prep failed", ex);
            throw ex;
        }
        if (lock.lock(300)) {
            try
            {
                result = DateraUtil.attachVolume(conn, storagePoolId, clusterId, appInstanceName);
                return result;
            }
            catch (Exception ex)
            {
                s_logger.error("Datera - DateraSharedPrimaryDataStoreDriver.grantAccess() failed", ex);
                throw ex;
            }
            finally
            {
                lock.unlock();
                lock.releaseRef();
            }
        }
        s_logger.debug("Couldn't lock the DB (in grantAccess) on the following string: " + cluster.getUuid());
        return false;
    }

    private boolean isDateraSupported(HypervisorType hypervisorType)
    {
        if(HypervisorType.VMware.equals(hypervisorType) || HypervisorType.XenServer.equals(hypervisorType))
            return true;
        return false;
    }
}
