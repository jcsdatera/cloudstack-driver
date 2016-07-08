package org.apache.cloudstack.storage.datastore.utils;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class DateraModel {

    public static final String defaultStorageName="storage-1";
    public static final String defaultVolumeName="volume-1";
    public static class LoginModel
     {
      public String name;
      public String password;
     }

     public static class ACLPolicyModel
     {
      public List<String> initiators; // e.g. "/initiators/iqn.1993-08.org.debian:01:6e3ca7bd74e1"
      @SerializedName("initiator_groups")
      public List<String> initiatorGroups; // e.g. "/initiator_groups/cluster2_initiator_group"

      public ACLPolicyModel(List<String> paramInitiators, List<String> paramInitiatorGroups)
      {
       initiators = paramInitiators;
       initiatorGroups = paramInitiatorGroups;
      }
     }
     public static class AppModel
     {
      public String name;
      @SerializedName("access_control_mode")
      public String accessControlMode;
      @SerializedName("storage_instances")
      public StorageInstanceModel storageInstances;
      @SerializedName("admin_state")
      public String adminState;

      public AppModel(String appInstanceName, String accessControlMode, StorageInstanceModel inst)
      {
       this.name = appInstanceName;
       this.accessControlMode = accessControlMode;
       this.storageInstances = inst;
      }
     }

     public static class StorageInstanceModel
     {
      @SerializedName(defaultStorageName)
      public StorageModel storage1;

      public StorageInstanceModel(StorageModel st)
      {
       storage1 = st;
      }
     }

     public static class StorageModel
     {
      public String name = defaultStorageName;
      public VolumeInstanceModel volumes;
      @SerializedName("acl_policy")
      public ACLPolicyModel aclPolicy;
      @SerializedName("ip_pool")
      public String ipPool;
      public StorageModel(String networkPoolName, VolumeInstanceModel vols, ACLPolicyModel policy)
      {
       volumes = vols;
       aclPolicy = policy;
       ipPool = networkPoolName;
      }
     }

     public static class VolumeInstanceModel
     {
      @SerializedName(defaultVolumeName)
      public VolumeModel volume1;

      public VolumeInstanceModel(VolumeModel vol)
      {
       volume1 = vol;
      }
     }
     public static class VolumeModel {

      public String name = defaultVolumeName;
      public int size;
      @SerializedName("replica_count")
      public int replicaCount;
      //snapshot policies

      public VolumeModel(String volName,int volSize, int volReplicaSize)
      {
       name =  volName;
       size = volSize;
       replicaCount = volReplicaSize;
      }
     }

     public static class StorageResponse
     {
      public class AccessControl
      {
       public List<String> ips;
       public String iqn;
      }

      public AccessControl access;

     }
     public static class AdminPrivilege
     {
       @SerializedName("admin_state")
       public String adminState;
       public boolean force;

       public AdminPrivilege(String paramAdminState)
       {
        adminState = paramAdminState;
       }
       public AdminPrivilege(String paramAdminState, boolean force)
       {
        this.adminState = paramAdminState;
        this.force = force;
       }

     }

     public static class GenericResponse
     {
      public String name;
      public String id;
     }

     public static class VolumeResize
     {
         public int size;
         public VolumeResize(int sz)
         {
           size = sz;
         }
     }

     public static class AppModelEx
     {
         public String name;

         public AppModelEx(String appName)
         {
           name = appName;
         }
     }

     public static class StorageModelEx
     {
         public String name;
         @SerializedName("ip_pool")
         public String ipPool;

         public StorageModelEx(String storageName, String ipPool)
         {
           name = storageName;
           this.ipPool = ipPool;
         }
     }
     public static class InitiatorModel
     {
        public String id;
        public String name;
       public InitiatorModel(String label, String iqn)
       {
         name = label;
         id = iqn;
       }
     }
     public static class DateraError
     {
        public String name;
        public int code;
        public int http;
        @SerializedName("errors")
        public String erros;
        public String message;
        @SerializedName("api_req_id")
        public int apiReqId;
        @SerializedName("storage_node_uuid")
        public String storageNodeUuid;
        public String ts;
     }

    public static class InitiatorGroup
     {
      public String name;
      public List<String> members = null;

      public InitiatorGroup(String paramName, List<String> paramMembers)
      {
       this.name = paramName;
       this.members = paramMembers;
      }
     }
    public static class PerformancePolicy
    {
       @SerializedName("read_iops_max")
       public long readIopsMax;
       @SerializedName("write_iops_max")
       public long writeIopsMax;
       @SerializedName("total_iops_max")
       public long totalIopsMax;

       @SerializedName("read_bandwidth_max")
       public long readBandwidthMax;
       @SerializedName("write_bandwidth_max")
       public long writeBandwidthMax;
       @SerializedName("total_bandwidth_max")
       public long totalBandwidthMax;

       public PerformancePolicy(long totalIOPS)
       {
           totalIopsMax = totalIOPS;
       }
    }
}
