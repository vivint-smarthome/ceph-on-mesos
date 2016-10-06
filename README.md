# requirements

- Zookeeper
- Mesos
- Docker
- XFS volumes

The OSD must run on an XFS formatted volume. It is highly recommended that you use mount disk resources or the OSD will
not be able to tell when it has used more than it's quota. If you launch an OSD daemon on a 


# API

## list tasks

```
curl http://127.0.0.1:8080/v1/tasks
```

## Update task goal

```
# pause a task
curl -X PUT http://127.0.0.1:8080/v1/tasks/aa76c126-16e8-4b43-a861-663332657f61/paused

# resume a task
curl -X PUT http://127.0.0.1:8080/v1/tasks/aa76c126-16e8-4b43-a861-663332657f61/running
```


# Updating configuration

Config updates to `(ceph)/ceph.conf` happen as follows:

- Increases to # of nodes to deploy are picked up immediately.
- Deployment resource config is picked up immediately, but USED FOR LAUNCHING new tasks only. Currently there is no
  support for changing resources for a task.
- Ceph configuration is picked up immediately, but applied as tasks are launched (IE - they are restarted).

Note that the latest configuration is deployed when a task is relaunched.
