// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.cloudstack.storage.datastore.utils;

import java.util.List;
import java.util.StringTokenizer;

import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.utils.DateraUtil.DateraMetaData;

import com.cloud.host.Host;
import com.cloud.host.HostVO;


public class DateraUtil {
    public static class DateraMetaData {
     public String mangementIP;
     public int managementPort;
     public String managementUserName;
     public String managementPassword;
     public String storagePoolName;
     public int replica;
     public String networkPoolName;
     public String appInstanceName;
     public String storageInstanceName;
     public String initiatorGroupName;
     public String clvmVolumeGroupName;
     public String storageVip2;

        public DateraMetaData(String ip, int port, String user, String pass, String storage,int paramReplica, String nwPoolName, String appInstanceName, String storageInstanceName, String initiatorGroupName,String clvmVolumeGroupName)
        {
            this.mangementIP = ip;
            this.managementPort = port;
            this.managementUserName = user;
            this.managementPassword = pass;
            this.storagePoolName = storage;
            this.replica = paramReplica;
            this.networkPoolName = nwPoolName;
            this.appInstanceName = appInstanceName;
            this.storageInstanceName = storageInstanceName;
            this.initiatorGroupName = initiatorGroupName;
            this.clvmVolumeGroupName = clvmVolumeGroupName;
        }
        public DateraMetaData(){}
    }
    public static final String PROVIDER_NAME = "Datera";
    public static final String SHARED_PROVIDER_NAME = "DateraShared";

    public static final String LOG_PREFIX = "Datera: ";

    public static final String STORAGE_POOL_NAME = "storagePoolName";

    public static final String DATACENTER = "datacenter";
    public static final String CLVM_VOLUME_GROUP_NAME = "volumeGroupName";

    public static final String MANAGEMENT_IP = "mgmtIP";

    public static final String MANAGEMENT_PORT = "mgmtPort";

    public static final String STORAGE_VIP_1 = "storageVIP1";
    public static final String STORAGE_VIP_2 = "storageVIP2";
    public static final String STORAGE_PORT = "storagePort";

    public static final String MANAGEMENT_USERNAME = "mgmtUserName";
    public static final String MANAGEMENT_PASSWORD = "mgmtPassword";

    public static final String APP_NAME = "appName";
    public static final String STORAGE_NAME = "storageName";
    public static final String INITIATOR_GROUP_NAME = "initiatorGroupName";

    public static final String VOLUME_REPLICA = "replica";
    public static final int MAX_VOLUME_REPLICA = 5;
    public static final int MIN_VOLUME_REPLICA = 1;
    public static final int DEFAULT_VOLUME_REPLICA = 3;

    public static final String NETWORK_POOL_NAME = "networkPoolName";
    public static final String MAX_TOTAL_IOPS = "maxTotalIOPs";
    public static final String MAX_READ_IOPS = "maxReadIOPs";
    public static final String MAX_WRITE_IOPS = "maxWriteIOPs";
    public static final String MAX_TOTAL_BANDWIDTH = "maxTotalBandwidth";
    public static final String MAX_READ_BANDWIDTH = "maxReadBandwidth";
    public static final String MAX_WRITE_BANDWIDTH = "maxWriteBandwidth";
    public static final String TOTAL_IOPS = "totalIOPS";
    public static final String ADMIN_STATE_ONLINE = "online";
    public static final String ADMIN_STATE_OFFLINE = "offline";


    //public static final long MAX_TOTAL_IOPS_PER_VOLUME = 100000;
    public static final long MAX_TOTAL_IOPS_PER_VOLUME = 10000000;
    public static final long MIN_TOTAL_IOPS_PER_VOLUME = 10;
    public static final long DEFAULT_TOTAL_IOPS_PER_VOLUME = 1000;

    private static final int DEFAULT_MANAGEMENT_PORT = 7718;
    public static final int DEFAULT_STORAGE_PORT = 3260;
    public static final long MIN_CAPACITY_BYTES = 1073741824L;
    public static final long MAX_CAPACITY_BYTES = 70368744177664L;
    public static final int VOLUME_CREATION_TIMEOUT = 30;
    public static final int WATING_INTERVAL = 2;
    public static final long PRIMARY_STORAGE_CREATION_TIMEOUT = 5000L;

    public static final String VOLUME_SIZE_NAME = "dateraVolumeSize";

    public static final String DATASTORE_NAME = "datastoreName";
    public static final String IQN = "iqn";
    private static final String DEFAULT_NETWORK_POOL_NAME = "default";
    public static final int LOCK_TIME_IN_SECOND = 300;


    public static boolean hostsSupport_iScsi(List<HostVO> hosts) {
        if (hosts == null || hosts.size() == 0) {
            return false;
        }

        for (Host host : hosts) {
            if (host == null || host.getStorageUrl() == null || host.getStorageUrl().trim().length() == 0 || !host.getStorageUrl().startsWith("iqn")) {
                return false;
            }
        }

        return true;
    }

    public static String getManagementIP(String url) {
        return getIP(DateraUtil.MANAGEMENT_IP, url);
    }


    public static int getManagementPort(String url) {
        return getPort(DateraUtil.MANAGEMENT_PORT, url);
    }

    private static String getIP(String keyToMatch, String url) {
        String delimiter = ":";

        String ip = getValue(keyToMatch, url);

        int index = ip.indexOf(delimiter);

        if (index != -1) {
            return ip.substring(0, index);
        }

        return ip;
    }

    private static int getPort(String keyToMatch, String url) {
        String port = getValue(keyToMatch, url);
        try {
            return Integer.parseInt(port);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid URL format (port is not an integer)");
        }
    }

    public static String getValue(String keyToMatch, String url) {
        return getValue(keyToMatch, url, true);
    }

    public static String getValue(String keyToMatch, String url, boolean throwExceptionIfNotFound) {
        String delimiter1 = ";";
        String delimiter2 = "=";

        StringTokenizer st = new StringTokenizer(url, delimiter1);

        while (st.hasMoreElements()) {
            String token = st.nextElement().toString();

            int index = token.indexOf(delimiter2);

            if (index == -1) {
                throw new RuntimeException("Invalid URL format");
            }

            String key = token.substring(0, index);

            if (key.trim().equalsIgnoreCase(keyToMatch)) {
                String valueToReturn = token.substring(index + delimiter2.length());

                return valueToReturn.trim();
            }
        }

        if (throwExceptionIfNotFound) {
            if (keyToMatch.equals(NETWORK_POOL_NAME)) {
                return DEFAULT_NETWORK_POOL_NAME;
            }
            else {
                throw new RuntimeException(keyToMatch + " not found in the specified url.");
            }
        }

        return null;
    }

    private static int getIntegerValue(String key,String url)
    {
        int value = 0;
        try
        {
            value = Integer.parseInt(DateraUtil.getValue(key, url));

        }catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid URL format (" + key + " is not an integer)");
        }
        return value;
    }

    public static int getReplica(String url)
    {
        return getIntegerValue(DateraUtil.VOLUME_REPLICA,url);
    }


    public static DateraUtil.DateraMetaData getDateraCred(long storagePoolId, StoragePoolDetailsDao storagePoolDetailsDao) {

        StoragePoolDetailVO storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.MANAGEMENT_IP);
        String managementIP = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.MANAGEMENT_PORT);
        int managementPort = (null != storagePoolDetail) ? Integer.parseInt(storagePoolDetail.getValue()) : 7718;

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.MANAGEMENT_USERNAME);
        String managementUserName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.MANAGEMENT_PASSWORD);
        String managementPassword = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.STORAGE_POOL_NAME);
        String storagePoolName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.VOLUME_REPLICA);
        int replica = (null != storagePoolDetail) ? Integer.parseInt(storagePoolDetail.getValue()) : 1;

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.NETWORK_POOL_NAME);
        String networkPoolName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.APP_NAME);
        String appInstanceName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.STORAGE_NAME);
        String storageInstanceName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.CLVM_VOLUME_GROUP_NAME);
        String clvmVolumeGroupName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.INITIATOR_GROUP_NAME);
        String initiatorGroupName = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        storagePoolDetail = storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.STORAGE_VIP_2);
        String storageVip2 = (null != storagePoolDetail) ? storagePoolDetail.getValue() : "";

        DateraMetaData dtMetaData = new DateraMetaData();
        dtMetaData.mangementIP = managementIP;
        dtMetaData.managementPort = managementPort;
        dtMetaData.managementUserName = managementUserName;
        dtMetaData.managementPassword = managementPassword;
        dtMetaData.storagePoolName = storagePoolName;
        dtMetaData.replica = replica;
        dtMetaData.networkPoolName = networkPoolName;
        dtMetaData.appInstanceName = appInstanceName;
        dtMetaData.storageInstanceName = storageInstanceName;
        dtMetaData.initiatorGroupName = initiatorGroupName;
        dtMetaData.clvmVolumeGroupName = clvmVolumeGroupName;
        dtMetaData.storageVip2 = storageVip2;
        return dtMetaData;
    }

    public static String generateAppInstanceName(String storagePoolName, String volumeUUD)
    {
       return storagePoolName+"_"+volumeUUD;
    }
    public static Long getVolumeSizeInBytes(long dtVolSize)
    {
         return (long)(dtVolSize*1024*1024*1024);
    }
    public static int getVolumeSizeInGB(Long csVolSize)
    {
        return (int)(csVolSize/(1024*1024*1024));
    }
    public static String constructInitiatorName(String hostIqn)
    {
        return "/initiators/"+hostIqn;
    }

    public static String constructVolumeName(String lunId) {
        return "volume-"+lunId;
    }

    public static String generateInitiatorGroupName(String appInstanceName) {

        return "csIG_" + appInstanceName;
    }
    public static String constructInitiatorLabel(String hostUUID)
    {
        return "cs_" + hostUUID;
    }
}
