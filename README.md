# requirements

- Zookeeper
- Mesos
- Docker
- XFS volumes

The OSD must run on an XFS formatted volume. It is highly recommended that you use mount disk resources or the OSD will
not be able to tell when it has used more than it's quota. If you launch an OSD daemon on a 
