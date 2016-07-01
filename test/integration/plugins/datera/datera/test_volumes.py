import logging
import random
import XenAPI
# All tests inherit from cloudstackTestCase
from marvin.cloudstackTestCase import cloudstackTestCase

# Import Integration Libraries

from marvin.lib.base import (Account, ServiceOffering, DiskOffering,
                             User, Volume, StoragePool, VirtualMachine)

# common - commonly used methods for all tests are listed here
from marvin.lib.common import (get_domain, get_zone, list_storage_pools,
                               list_hosts, list_clusters,
                               list_volumes, list_templates)

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
    diskOffering = "diskoffering"
    diskSize = "disksize"
    diskName = "diskName"
    domainId = "domainId"
    hypervisor = "hypervisor"
    login = "login"
    mvip = "mvip"
    name = "name"
    newHost = "newHost"
    newHostDisplayName = "newHostDisplayName"
    osName = "ostype"
    password = "password"
    podId = "podid"
    port = "port"
    primaryStorage = "primarystorage"
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

    def __init__(self):
        self.datear_url = (
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
            TestData.account: {
                "email": "test@example.com",
                "firstname": "John",
                "lastname": "Doe",
                TestData.username: "test",
                TestData.password: "test"
            },
            TestData.user: {
                "email": "user@example.com",
                "firstname": "Jane",
                "lastname": "Doe",
                TestData.username: "testuser",
                TestData.password: "password"
            },
            TestData.primaryStorage: {
                TestData.name: "datera-%d" % random.randint(0, 100),
                TestData.scope: "CLUSTER",
                TestData.url: self.datear_url[0],
                TestData.provider: "DateraShared",
                TestData.tags: TestData.storageTag,
                TestData.capacityIops: 10000,
                TestData.capacityBytes: 10737418240,
                TestData.hypervisor: "XenServer",
                TestData.podId: 1
            },
            TestData.diskOffering: {
                "name": "Datera_DO",
                "displaytext": "Datera_DO",
                "disksize": 1,
                "miniops": 300,
                "maxiops": 500,
                TestData.tags: TestData.storageTag,
                "storagetype": "shared"
            },
            TestData.volume_1: {
                TestData.diskName: "testvolume",
            },
            TestData.virtualMachine: {
                TestData.name: "TestVM",
                "displayname": "Test VM"
            },
            TestData.computeOffering: {
                TestData.name: "Datera",
                TestData.displayText: "datera",
                "cpunumber": 1,
                "cpuspeed": 100,
                "memory": 512,
                "storagetype": "shared",
                "miniops": "10000",
                "maxiops": "15000",
                TestData.tags: TestData.storageTag
            },
            TestData.osName: "Debian basic webserver (Xenserver)",
            TestData.zoneId: 1,
            TestData.clusterId: 1,
            TestData.domainId: 1,
            TestData.url: "172.19.175.174",
            TestData.clusterName: "Cluster-Xen",
            TestData.hostName: "tlx167.tlx.daterainc.com"
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

        list_template_response = list_templates(cls.apiClient,
                                                zoneid=cls.zone.id,
                                                templatefilter='all')
        for templates in list_template_response:
            if templates.name == cls.testdata[TestData.osName]:
                cls.template = templates
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

        # Create test account
        cls.account = Account.create(
            cls.apiClient,
            cls.testdata[TestData.account],
            admin=1
        )

        # Set up connection to make customized API calls
        user = User.create(
            cls.apiClient,
            cls.testdata[TestData.user],
            account=cls.account.name,
            domainid=cls.domain.id
        )

        cls.compute_offering = ServiceOffering.create(
            cls.apiClient,
            cls.testdata[TestData.computeOffering]
        )

        cls.disk_offering = DiskOffering.create(
            cls.apiClient,
            cls.testdata[TestData.diskOffering]
        )

        cls._cleanup = [
            cls.compute_offering,
            cls.disk_offering,
            user,
            cls.account
        ]

    @classmethod
    def tearDownClass(cls):
        try:
            cleanup_resources(cls.apiClient, cls._cleanup)
        except Exception as e:
            logging.debug("Exception in tearDownClass(cls): %s" % e)

    def setUp(self):
        self.virtual_machine = None
        self.cleanup = []

    def tearDown(self):
        try:
            if self.virtual_machine is not None:
                self.virtual_machine.delete(self.apiClient, True)

            cleanup_resources(self.apiClient, self.cleanup)
        except Exception as e:
            logging.debug("Exception in tearDown(self): %s" % e)

    def test09_add_vm_with_datera_storage(self):
        primarystorage = self.testdata[TestData.primaryStorage]

        primary_storage = StoragePool.create(
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

        primary_storage_url = primarystorage[TestData.url]
        self._verify_attributes(
            primary_storage.id, primary_storage_url)

        self.cleanup.append(primary_storage)
        self.virtual_machine = VirtualMachine.create(
            self.apiClient,
            self.testdata[TestData.virtualMachine],
            accountid=self.account.name,
            zoneid=self.zone.id,
            serviceofferingid=self.compute_offering.id,
            templateid=self.template.id,
            domainid=self.domain.id,
            startvm=True
        )

        self._validate_storage(primary_storage, self.virtual_machine)

    def test10_add_vm_with_datera_storage_and_volume(self):
        primarystorage = self.testdata[TestData.primaryStorage]

        primary_storage = StoragePool.create(
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

        primary_storage_url = primarystorage[TestData.url]
        self._verify_attributes(
            primary_storage.id, primary_storage_url)

        self.cleanup.append(primary_storage)
        self.virtual_machine = VirtualMachine.create(
            self.apiClient,
            self.testdata[TestData.virtualMachine],
            accountid=self.account.name,
            zoneid=self.zone.id,
            serviceofferingid=self.compute_offering.id,
            templateid=self.template.id,
            domainid=self.domain.id,
            startvm=True
        )

        self._validate_storage(primary_storage, self.virtual_machine)

        volume = Volume.create(
            self.apiClient,
            self.testdata[TestData.volume_1],
            account=self.account.name,
            domainid=self.domain.id,
            zoneid=self.zone.id,
            diskofferingid=self.disk_offering.id
        )

        virtual_machine.attach_volume(
            self.apiClient,
            volume
        )
        storage_pools_response = list_storage_pools(
            self.apiClient, id=primary_storage.id)

        for key, value in self.xen_session.xenapi.SR.get_all_records().items():
            if value['name_description'] == primary_storage.id:
                xen_server_response = value

        self.assertNotEqual(
            int(storage_pools_response[0].disksizeused),
            int(xen_server_response['physical_utilisation']))

    def _validate_storage(self, storage, vm):
        list_volumes_response = list_volumes(
            self.apiClient, virtualmachineid=vm.id, listall=True)

        self.assertNotEqual(
            list_volumes_response, None,
            "'list_volumes_response' should not be equal to 'None'.")

        for volume in list_volumes_response:
            if volume.type.upper() == "ROOT":
                volumeData = volume
                self.assertEqual(volume.storage, storage.name,
                                 "Volume not created for VM " + str(vm.id))
        #Verify in cloudstack
        storage_pools_response = list_storage_pools(
            self.apiClient, id=storage.id)

        self.assertEqual(
            int(volumeData.size),
            int(storage_pools_response[0].disksizeused),
            "Allocated disk sizes are not same in volumes and primary stoarge")

        #Verify in datera
        datera_primarystorage_name = "cloudstack-" + storage.id
        for instance in self.datera_api.app_instances.list():
            if instance['name'] == datera_primarystorage_name:
                app_instance_response = instance

        app_instance_response_disk = (
            app_instance_response['storage_instances']
            ['storage-1']['volumes']['volume-1']
            ['capacity_in_use'] * 1073741824)
        self.assertEqual(
            int(volumeData.size),
            int(app_instance_response_disk),
            "App instance usage size is incorrect")

        #Verify in xen server
        for key, value in self.xen_session.xenapi.SR.get_all_records().items():
            if value['name_description'] == storage.id:
                xen_server_response = value
        self.assertEqual(
            int(xen_server_response['physical_utilisation']),
            int(volumeData.size),
            "Allocated disk sizes is incorrect in xenserver")

    def _verify_attributes(self, primarystorage_id, primary_storage_url):
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

        if cs_netPool:
            cs_netPool = ''.join(cs_netPool).split("=")[1]
            self.assertEqual(
                app_instance_response_ippool.split('/')[2],
                cs_netPool,
                "Incorrect networkPoolName")

        if not cs_netPool:
            self.assertEqual(
                app_instance_response_ippool.split('/')[2],
                'default',
                "Incorrect networkPool Name")
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
