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
     - https://github.com/Datera/cloudstack/cloud-plugin-storage-volume-datera-4.5.3-SNAPSHOT.jar

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
   * - ``mgmtIP`` = ``None``
     - (String) Datera API port.
   * - ``mgmtPort`` = ``7717``
     - (String) Datera API port.
   * - ``mgmtUserName`` = ``None``
     - (String) Datera API user name.
   * - ``mgmtPassword`` = ``None``
     - (String) Datera API user password.
   * - ``networkPoolName`` = ``default``
     - (String) Datera access network pool name.
   * - ``replica`` = ``1``
     - (Int) Number of replicas to create of an inode.
   * - ``timeout`` = ``10000``
     - (Int) Number of milliseconds to pause after creating a volume.
