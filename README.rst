====================================
DATERA CloudStack Driver Repository
====================================
.. list-table:: CloudStack Driver Version with Datera Product and Supported Hypervisor(s)
   :header-rows: 1
   :class: version-table

   * - CloudStack Release
     - Driver Version
     - Capabilities Introduced
     - Supported Datera Product Versions
     - Supported Hypervisors
     - Driver URL
   * - 4.5.2
     - v1.0
     - Shared Primary Storage
     - 1.1
     - XenServer 6.2
     - https://raw.githubusercontent.com/Datera/cloudstack-driver/master/cloud-plugin-storage-volume-datera-4.5.2.jar
   * - 4.7.2
     - v1.0
     - Shared Primary Storage
     - 1.1
     - XenServer 6.5
     - https://raw.githubusercontent.com/Datera/cloudstack-driver/master/cloud-plugin-storage-volume-datera-4.7.2.jar
   * - 4.7.2
     - v2.0
     - Dynamic Primary Storage
     - 2.1
     - XenServer 6.5
       CentOS 7.2
     - https://raw.githubusercontent.com/Datera/cloudstack-driver/master/cloud-plugin-storage-volume-datera-4.7.2-v2.0.jar
   * - 4.8.2
     - v2.0
     - Dynamic Primary Storage
     - 2.1
     - XenServer 6.5
     - https://raw.githubusercontent.com/Datera/cloudstack-driver/master/cloud-plugin-storage-volume-datera-4.8.2-v2.0.jar
   * - 4.9.2
     - v2.0
     - Dynamic Primary Storage
     - 2.1
     - XenServer 6.5
       CentOS 7.2
     - https://raw.githubusercontent.com/Datera/cloudstack-driver/master/cloud-plugin-storage-volume-datera-4.9.0-v2.0.jar

======================
Configuration Options
======================

.. list-table:: Description of Datera CloudStack driver configuration options
   :header-rows: 1
   :class: config-ref-table

   * - Configuration option = Default value
     - Description
   * - **[DEFAULT]**
     -
   * - ``MVIP`` = ``None``
     - (String) Datera API management vip.
   * - ``SVIP`` = ``None``
     - (String) Datera Access vip.
   * - ``clusterAdminUsername`` = ``None``
     - (String) Datera API user name.
   * - ``clusterAdminPassword`` = ``None``
     - (String) Datera API user password.
   * - ``numReplicas`` = ``2``
     - (Int) Number of replicas to create of a volume.

===================
Configuration Steps
===================

1. Deploy CloudStack by following the instructions http://docs.cloudstack.apache.org/projects/cloudstack-installation/en/4.9/
2. Download Datera CloudStack driver from https://raw.githubusercontent.com/Datera/cloudstack-driver/master/cloud-plugin-storage-volume-datera-4.9.0-v2.0.jar
3. Save it to 

   ``/usr/share/cloudstack-management/webapps/client/WEB-INF/lib/``
4. Restart CloudStack management service

   ``service cloudstack-management restart``

