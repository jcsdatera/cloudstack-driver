#import time
import logging
import random
import XenAPI
# All tests inherit from cloudstackTestCase
from marvin.cloudstackTestCase import cloudstackTestCase

# Import Integration Libraries

from marvin.lib.base import ServiceOffering, StoragePool

# common - commonly used methods for all tests are listed here
from marvin.lib.common import (get_domain, get_zone,
                               list_hosts, list_clusters,
                               list_storage_pools)

# utils - utility classes for common cleanup, external library wrappers, etc.
from marvin.lib.utils import cleanup_resources

from dfs_sdk import DateraApi


class TestData:
    account = "account"
    capacityBytes = "capacitybytes"
    capacityIops = "capacityiops"
    clusterId = "clusterId"
    computeOffering = "computeoffering"
    displayText = "displaytext"
    diskSize = "disksize"
    domainId = "domainId"
    hypervisor = "hypervisor"
    login = "login"
    mvip = "mvip"
    name = "name"
    newHost = "newHost"
    newHostDisplayName = "newHostDisplayName"
    osType = "ostype"
    password = "password"
    podId = "podid"
    port = "port"
    primaryStorage = "primarystorage"
    primaryStorage2 = "primarystorage2"
    provider = "provider"
    scope = "scope"
    Datera = "Datera"
    storageTag = "Datera_1"
    storageTag2 = "datera_2"
    tags = "tags"
    url = "url"
    path = "path"
    urlOfNewHost = "urlOfNewHost"
    user = "user"
    username = "username"
    virtualMachine = "virtualmachine"
    volume_1 = "volume_1"
    xenServer = "xenserver"
    zoneId = "zoneid"
    clusterName = "clusterName"
    hostName = "hostname"
    newCapacityBytes = "update_capacityBytes"
    newCapacityIops = "capacityIops"

    def __init__(self):
        self.datera_url = (
            "mgmtIP=172.19.2.214;mgmtPort=7718;" +
            "mgmtUserName=admin;mgmtPassword=password;" +
            "replica=3;networkPoolName=default"),
        self.testdata = {
            TestData.Datera: {
                TestData.mvip: "172.19.2.214",
                TestData.login: "admin",
                TestData.password: "password",
            },
            TestData.xenServer: {
                TestData.username: "root",
                TestData.password: "maple"
            },
            TestData.primaryStorage: {
                TestData.name: "Datera-%d" % random.randint(0, 100),
                TestData.scope: "CLUSTER",
                TestData.url: self.datera_url[0],
                TestData.provider: "DateraShared",
                TestData.tags: TestData.storageTag,
                TestData.capacityIops: 500,
                TestData.capacityBytes: 1073741824,
                TestData.hypervisor: "XenServer",
                TestData.podId: 1
            },
            TestData.computeOffering: {
                TestData.name: "DATERA",
                TestData.displayText: (
                    "DATERA (Min IOPS = 10,000; Max IOPS = 15,000)"),
                "cpunumber": 1,
                "cpuspeed": 100,
                "memory": 128,
                "storagetype": "shared",
                "customizediops": False,
                "miniops": "10000",
                "maxiops": "15000",
                "hypervisorsnapshotreserve": 200,
                TestData.tags: TestData.storageTag
            },
            TestData.zoneId: 1,
            TestData.clusterId: 1,
            TestData.domainId: 1,
            TestData.url: "172.19.175.174",
            TestData.clusterName: "Cluster-Xen",
            TestData.hostName: "tlx167.tlx.daterainc.com",
            TestData.newCapacityBytes: 2147483648,
            TestData.newCapacityIops: 1000
        }


class TestPrimaryStorage(cloudstackTestCase):

    @classmethod
    def setUpClass(cls):
        # Set up API client
        testclient = super(TestPrimaryStorage, cls).getClsTestClient()
        cls.apiClient = testclient.getApiClient()
        cls.dbConnection = testclient.getDbConnection()
        cls.services = testclient.getParsedTestDataConfig()
        cls.testdata = TestData().testdata
        cls.zone = get_zone(
            cls.apiClient, zone_id=cls.testdata[TestData.zoneId])
        for cluster in list_clusters(cls.apiClient):
            if cluster.name == cls.testdata[TestData.clusterName]:
                cls.cluster = cluster
        cls.domain = get_domain(cls.apiClient, cls.testdata[TestData.domainId])
        cls.xs_pool_master_ip = list_hosts(
            cls.apiClient, clusterid=cls.cluster.id)
        for host in cls.xs_pool_master_ip:
            if host.name == cls.testdata[TestData.hostName]:
                cls.xs_pool_master_ip = host.ipaddress
        host_ip = "https://" + cls.xs_pool_master_ip
        cls.xen_session = XenAPI.Session(host_ip)
        xenserver = cls.testdata[TestData.xenServer]
        cls.xen_session.xenapi.login_with_password(
            xenserver[TestData.username], xenserver[TestData.password])
        datera = cls.testdata[TestData.Datera]
        cls.datera_api = DateraApi(
            username=datera[TestData.login],
            password=datera[TestData.password],
            hostname=datera[TestData.mvip])

        cls.compute_offering = ServiceOffering.create(
            cls.apiClient,
            cls.testdata[TestData.computeOffering]
        )

        cls._cleanup = [cls.compute_offering]

    @classmethod
    def tearDownClass(cls):
        try:
            cleanup_resources(cls.apiClient, cls._cleanup)
        except Exception as e:
            logging.debug("Exception in tearDownClass(cls): %s" % e)

    def setUp(self):
        primarystorage = self.testdata[TestData.primaryStorage]

        self.primary_storage = StoragePool.create(
            self.apiClient,
            primarystorage,
            scope=primarystorage[TestData.scope],
            zoneid=self.zone.id,
            clusterid=self.cluster.id,
            provider=primarystorage[TestData.provider],
            tags=primarystorage[TestData.tags],
            capacityiops=primarystorage[TestData.capacityIops],
            capacitybytes=primarystorage[TestData.capacityBytes],
            hypervisor=primarystorage[TestData.hypervisor]
        )
        self.primary_storage_id = self.primary_storage.id
        self._primary_storage = [self.primary_storage]
        self.primary_tag = primarystorage[TestData.tags]
        self.cleanup = [self.primary_storage]
        primary_storage_url = primarystorage[TestData.url]
        self._verify_priamry_storage(
            self.primary_storage_id, primary_storage_url)

    def tearDown(self):
        try:
            if len(self.cleanup) > 0:
                cleanup_resources(self.apiClient, self.cleanup)
        except Exception as e:
            logging.debug("Exception in tearDown(self): %s" % e)

    def test04_delete_primary_storage(self):
        #cleanup_resources(self.apiClient, self._primary_storage)
        StoragePool.delete(self.primary_storage, self.apiClient)
        self.cleanup = []

        # Verify in Cloudstack
        storage_pools_response = list_storage_pools(
            self.apiClient, clusterid=self.cluster.id)
        for storage in storage_pools_response:
            self.assertNotEqual(
                storage.id,
                self.primary_storage_id,
                "Primary storage not deleted")

        # Verify in Datera
        flag = 0
        datera_primary_storage_name = "cloudstack-" + self.primary_storage_id
        for item in self.datera_api.app_instances.list():
            if item['name'] == datera_primary_storage_name:
                flag = 1
        self.assertEqual(flag, 0, "app instance not deleted.")

        # Verify in xenserver
        for key, value in self.xen_session.xenapi.SR.get_all_records().items():
            self.assertNotEqual(
                value['name_description'],
                self.primary_storage_id,
                "SR not deleted in xenserver")

        # Verify in sql database
        command = "select uuid from storage_pool"
        sql_result = self.dbConnection.execute(command)
        key = 0
        for uuid in sql_result:
            if uuid[0] == self.primary_storage_id:
                key = 1
        self.assertEqual(
            key, 0, "Primary storage not deleted in database")

    def test05_primary_storage_enable_maintenance_mode(self):
        StoragePool.enableMaintenance(self.apiClient,
                                      id=self.primary_storage_id)

        # Verify in cloudsatck
        storage_pools_response = list_storage_pools(
            self.apiClient, clusterid=self.cluster.id)
        for storage in storage_pools_response:
            if storage.id == self.primary_storage_id:
                storage_pool = storage

        # Verify in datera
        datera_primary_storage_name = "cloudstack-" + self.primary_storage_id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primary_storage_name:
                datera_instance = instance

       # Verify in xenserver
       #for key, value in self.xen_session.xenapi.SR.get_all_records().items():
       #    if value['name_description'] == self.primary_storage_id:
       #        xen_sr = value

        try:
            self.assertEqual(
                storage_pool.state, "Maintenance",
                "Primary storage not in maintenance mode")

            self.assertEqual(
                datera_instance["admin_state"], "offline",
                "app-instance not in offline mode")

           # self.assertEqual(
           #     set(["forget", "destroy"])
           #         .issubset(xen_sr["allowed_operations"]),
           #     True, "Xenserver SR not in offline mode")
        except Exception as e:
            StoragePool.cancelMaintenance(
                self.apiClient, id=self.primary_storage_id)
            raise e

        StoragePool.cancelMaintenance(self.apiClient,
                                      id=self.primary_storage_id)
        StoragePool.delete(self.primary_storage, self.apiClient)
        self.cleanup = []

    def test06_primary_storage_cancel_maintenance_mode(self):
        StoragePool.enableMaintenance(self.apiClient,
                                      id=self.primary_storage_id)
        StoragePool.cancelMaintenance(self.apiClient,
                                      id=self.primary_storage_id)

        # Verify in cloudsatck
        storage_pools_response = list_storage_pools(
            self.apiClient, clusterid=self.cluster.id)
        for storage in storage_pools_response:
            if storage.id == self.primary_storage_id:
                storage_pool = storage
        self.assertEqual(
            storage_pool.state, "Up",
            "Primary storage not in up mode")

        # Verify in datera
        datera_primary_storage_name = "cloudstack-" + self.primary_storage_id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primary_storage_name:
                datera_instance = instance
        self.assertEqual(
            datera_instance["admin_state"], "online",
            "app-instance not in online mode")

        # Verify in xenserver
        for key, value in self.xen_session.xenapi.SR.get_all_records().items():
            if value['name_description'] == self.primary_storage_id:
                xen_sr = value
        self.assertEqual(
            set(["forget", "destroy"]).issubset(xen_sr["allowed_operations"]),
            False, "Xenserver SR in offline mode")

        StoragePool.delete(self.primary_storage, self.apiClient)
        self.cleanup = []

    def test07_update_primary_storage_capacityBytes(self):
        updatedDiskSize = self.testdata[TestData.newCapacityBytes]
        StoragePool.update(self.apiClient,
                           id=self.primary_storage_id,
                           capacitybytes=updatedDiskSize,
                           tags=self.primary_tag)

        # Verify in cloudsatck
        storage_pools_response = list_storage_pools(
            self.apiClient, clusterid=self.cluster.id)
        for data in storage_pools_response:
            if data.id == self.primary_storage_id:
                storage_pool = data

        self.assertEqual(
            storage_pool.disksizetotal, updatedDiskSize,
            "Primary storage not updated")

        # Verify in datera
        datera_primary_storage_name = "cloudstack-" + self.primary_storage_id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primary_storage_name:
                datera_instance = instance
        app_instance_response_disk_size = (
            datera_instance['storage_instances']
            ['storage-1']['volumes']['volume-1']['size'] * 1073741824)

        self.assertEqual(
            app_instance_response_disk_size, updatedDiskSize,
            "app-instance not updated")

        # Verify in xenserver
       #for key, value in self.xen_session.xenapi.SR.get_all_records().items():
        #    if value['name_description'] == self.primary_storage_id:
        #        xen_sr = value
        #Uncomment after xen fix
        #print xen_sr
        #print xen_sr['physical_size'], updatedDiskSize
        #self.assertEqual(
        #    int(xen_sr['physical_size']) + 12582912, updatedDiskSize,
        #    "Xen server physical storage not updated")

        StoragePool.delete(self.primary_storage, self.apiClient)
        self.cleanup = []

    def test08_update_primary_storage_capacityIops(self):
        updatedIops = self.testdata[TestData.newCapacityIops]
        StoragePool.update(self.apiClient,
                           id=self.primary_storage_id,
                           capacityiops=updatedIops,
                           tags=self.primary_tag)

        # Verify in cloudsatck
        storage_pools_response = list_storage_pools(
            self.apiClient, clusterid=self.cluster.id)
        for data in storage_pools_response:
            if data.id == self.primary_storage_id:
                storage_pool = data

        self.assertEqual(
            storage_pool.capacityiops, updatedIops,
            "Primary storage capacityiops not updated")

        # Verify in datera
        datera_primary_storage_name = "cloudstack-" + self.primary_storage_id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primary_storage_name:
                datera_instance = instance
        app_instance_response_iops = (
            datera_instance['storage_instances']
            ['storage-1']['volumes']['volume-1']['performance_policy']
            ['total_iops_max'])

        self.assertEqual(
            app_instance_response_iops, updatedIops,
            "app-instance capacityiops not updated")

        StoragePool.delete(self.primary_storage, self.apiClient)
        self.cleanup = []

    def test13_update_primary_storage_capacityIops_to_zero(self):
        updatedIops = 0
        StoragePool.update(self.apiClient,
                           id=self.primary_storage_id,
                           capacityiops=updatedIops,
                           tags=self.primary_tag)

        # Verify in cloudsatck
        storage_pools_response = list_storage_pools(
            self.apiClient, clusterid=self.cluster.id)
        for data in storage_pools_response:
            if data.id == self.primary_storage_id:
                storage_pool = data

        self.assertEqual(
            storage_pool.capacityiops, updatedIops,
            "Primary storage capacityiops not updated")

        # Verify in datera
        datera_primary_storage_name = "cloudstack-" + self.primary_storage_id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primary_storage_name:
                datera_instance = instance
        app_instance_response_iops = (
            datera_instance['storage_instances']
            ['storage-1']['volumes']['volume-1']['performance_policy']
            ['total_iops_max'])

        self.assertEqual(
            app_instance_response_iops, updatedIops,
            "app-instance capacityiops not updated")

        StoragePool.delete(self.primary_storage, self.apiClient)
        self.cleanup = []

    def _verify_priamry_storage(self, primarystorage_id, primary_storage_url):
        #Cloudstack Primary storage pool
        storage_pools_response = list_storage_pools(
            self.apiClient, id=primarystorage_id)
        storage_pools_response = storage_pools_response[0]
        storage_pools_response_type = storage_pools_response.type
        storage_pools_response_iqn = storage_pools_response.path
        storage_pools_response_disk = storage_pools_response.disksizetotal
        storage_pools_response_ipaddress = storage_pools_response.ipaddress
        storage_pools_response_state = storage_pools_response.state
        storage_pools_response_iops = storage_pools_response.capacityiops

        self.assertEqual(
            storage_pools_response_type, "IscsiLUN",
            "Failed to create IscsiLUN")

        self.assertEqual(
            storage_pools_response_state, "Up",
            "Primary storage status is down")

        # Datera app instances
        datera_primarystorage_name = "cloudstack-" + primarystorage_id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primarystorage_name:
                app_instance_response = instance
        app_instance_response_iqn = (
            app_instance_response['storage_instances']
            ['storage-1']['access']['iqn'])
        app_instance_response_ipaddress = (
            app_instance_response['storage_instances']
            ['storage-1']['access']['ips'])
        app_instance_response_op_state = (
            app_instance_response['storage_instances']
            ['storage-1']['volumes']['volume-1']['op_state'])
        app_instance_response_disk = (
            app_instance_response['storage_instances']
            ['storage-1']['volumes']['volume-1']['size'] * 1073741824)
        app_instance_response_iops = (
            app_instance_response['storage_instances']
            ['storage-1']['volumes']['volume-1']['performance_policy']
            ['total_iops_max'])
        app_instance_response_replica = (
            app_instance_response['storage_instances']
            ['storage-1']['volumes']['volume-1']['replica_count'])
        app_instance_response_ippool = (
            app_instance_response['storage_instances']
            ['storage-1']['ip_pool'])

        cs_replica = [data for data in primary_storage_url.split(";")
                      if data.startswith('replica')]
        cs_netPool = [data for data in primary_storage_url.split(";")
                      if data.startswith('networkPoolName')]

        if cs_replica:
            replica_count = ''.join(cs_replica).split("=")[1]

            self.assertEqual(
                int(replica_count),
                int(app_instance_response_replica),
                "Incorrect replicas count")
        if not cs_replica:
            self.assertEqual(
                int(3),
                int(app_instance_response_replica),
                "Incorrect replicas count")

        # Verify in sql database
        command = (
            "select status,id  from storage_pool where uuid='" +
            primarystorage_id + "'")
        sql_result = self.dbConnection.execute(command)

        self.assertEqual(
            sql_result[0][0], 'Up',
            "Priamry storage not added in database")

        command2 = (
            "select * from storage_pool_details where pool_id='" +
            str(sql_result[0][1]) + "'")

        sql_result1 = self.dbConnection.execute(command2)
        flag = 0
        for data in sql_result1:
            for record in data:
                if record == datera_primarystorage_name:
                    flag = 1
        self.assertEqual(
            flag, 1,
            "Priamry storage not added in database")

        self.assertEqual(
            app_instance_response_op_state, "available",
            "datera app instance is down")

        # xen server details
        for key, value in self.xen_session.xenapi.SR.get_all_records().items():
            if value['name_description'] == primarystorage_id:
                xen_server_response = value

        #xen_server_response_iqn = xen_server_response['name_description']
        xen_server_response_type = xen_server_response['type']
        xen_server_physical_size = xen_server_response['physical_size']

        if (storage_pools_response_ipaddress not in
                app_instance_response_ipaddress):
            self.assert_(False, "Storage ip address are different")

        self.assertEqual(
            xen_server_response_type, "lvmoiscsi",
            "Failed to craete lvmoiscsi on xen server")

        self.assertEqual(
            storage_pools_response_iqn.split("/")[1],
            app_instance_response_iqn,
            "Iqn values mismatch")

        self.assertEqual(
            app_instance_response_disk,
            storage_pools_response_disk,
            "Disk sizes are different")

        self.assertEqual(
            int(xen_server_physical_size) + 12582912,
            storage_pools_response_disk,
            "Disk sizes are not same in xen and cloudsatck")

        self.assertEqual(
            storage_pools_response_iops,
            app_instance_response_iops,
            "IOPS values are incorrect")

        cs_netPool = ''.join(cs_netPool).split("=")[1]
        self.assertEqual(
            app_instance_response_ippool.split('/')[2],
            cs_netPool,
            "Incorrect networkPoolName")
