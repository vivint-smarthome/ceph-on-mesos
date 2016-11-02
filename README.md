# Ceph on Mesos

Ceph on Mesos is working Mesos Framework with a boring name. It can be used to reliably deploy and manage a persistent,
Ceph cluster.

Some highlights:

- Orchestrated bootstrap and deployment.
- Reserves and launches Monitors and OSDs on reserved resources. This keeps other tasks from taking resources away from
  an OSD when it restarts.
- Low dependencies.
- Launch OSD containers in a "paused" state to support manual intervention.

For a list of planned features (and progress thus far), see the [TODO.md](./TODO.md) file in this repository.

# Requirements

- Mesos
- Docker (the ceph-docker image is used)
- Zookeeper (mesos master discovery, exclusive framework lock, state store)
- XFS formatted mount-disk resources for
  [OSDs](http://docs.ceph.com/docs/jewel/rados/configuration/filesystem-recommendations/).

# Deployment

If you have the JVM installed on some or all of your slaves, you can launch Ceph-on-Mesos using the Mesos
Containerizer. You may also package the artifact up in a docker container of your choice. It is easiest to use host
networking, although so long as the framework has an IP which the Mesos masters can directly reach, you may use any
networking abstraction you like.

## Step 1: Deploy the framework

Here is an example of a Marathon job which deploys the artifact directly using the Mesos containerizer:

```
{
  "id": "/frameworks/ceph",
  "cmd": "cd ceph-on-mesos-*\nbin/ceph-on-mesos --api-port=$PORT0",
  "cpus": 0.1,
  "mem": 512,
  "disk": 0,
  "instances": 1,
  "env": {
    "MESOS_ROLE": "ceph",
    "MESOS_PRINCIPAL": "ceph",
    "MESOS_SECRET": "your-principal-super-secret",
    "PUBLIC_NETWORK": "172.0.0.0/24",
    "CLUSTER_NETWORK": "172.0.0.0/24",
    "ZOOKEEPER": "172.0.0.11:2181,172.0.0.12:2181,172.0.0.13:2181,172.0.0.14:2181,172.0.0.15:2181/",
    "API_HOST": "0.0.0.0",
    "MESOS_MASTER": "zk://172.0.0.11:2181,172.0.0.12:2181,172.0.0.13:2181,172.0.0.14:2181,172.0.0.15:2181/mesos"
  },
  "uris": ["https://dl.bintray.com/vivint-smarthome/ceph-on-mesos/ceph-on-mesos-0.2.6.tgz"],
  "portDefinitions": [{"protocol": "tcp", "name": "api"}],
  "healthChecks": [
    {
      "path": "/v1/tasks",
      "protocol": "HTTP",
      "portIndex": 0,
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "timeoutSeconds": 20,
      "maxConsecutiveFailures": 3,
      "ignoreHttp1xx": false
    }
  ],
  "upgradeStrategy": {
    "minimumHealthCapacity": 0,
    "maximumOverCapacity": 0
  },
  "labels": {
    "MARATHON_SINGLE_INSTANCE_APP": "true"
  }
}
```

## Step 2: Update the default configuration to launch monitors

Once the framework is deployed, it create populate a default configuration file to ZooKeeper in the node
`/ceph-on-mesos/ceph.conf`. You can either use the web-UI (point your browser to the marathon assigned host and port if
you used the job specification above), or use your favorite zookeeper editor (Exhibitor,
[zk-web](https://github.com/qiuxiafei/zk-web), etc.) to edit it. Set the resource requirements and update the mon count
to 3 (as seen below). Save it.

```
deployment {
  mon {
    count = 3
    cpus = 1
    mem = 1024.0

    # # The type of multi-disk volume to use; valid values are root, path, and mount.
    disk_type = root

    # # Size of persistent volume. In the case of diskType = mount, the minimum size of disk to allocate.
    disk = 40960
  }

...
```

You can find the default configuration in this repository at
[src/main/resources/deployment-config.conf](https://github.com/vivint-smarthome/ceph-on-mesos/blob/master/src/main/resources/deployment-config.conf).

The framework detects changes to this configuration and will automatically deploy them. Don't panic! It will only add
nodes in response to a config change, never remove them. Watch the framework logs (go to the framework's mesos sandbox,
open stdout). If there are issues matching resources, you will see errors in the log. By default, `ceph-on-mesos` will
only deploy one monitor per host.

Once the monitors are deploy, you can join any of the docker images launched, and run `ceph -s`. If successful, you'll
see a status which says "3 mons at ...". If no docker containers launched, check the Ceph on Mesos stdout output for
errors. You can watch the status of jobs by opening the UI (again, if you followed the Marathon config above, then it is
the port assigned to the task); alternatively, you can issue a request directly to the REST API to `GET /v1/jobs`.

## Step 3: Update the default configuration to launch OSDs



```
deployment {
  ...

  osd {
    # # Number of OSD instances to spawn
    count = 6

    cpus = 4

    mem = 4096
    # # The type of multi-disk volume to use for the persistent volume; valid values are root, path, and mount.
    disk_type = mount

    # # Size of persistent volume. In the case of diskType = mount, the minimum size of disk to allocate. It is heavily
    # # ill-advised to use anything except mount disks for OSDs.
    disk = 4700000

    # # For diskType = mount, don't allocate drives larger than this.

    # disk_max = 1048576

    # # pathConstraint will tell the ceph framework to only allocate persistent mount volumes at a path which FULLY
    # # matches the provided regular expression (I.E. pretend an implicit '^' is added at the beginning of your regex
    # # and a '$' at the end).

    # path_constraint = "/mnt/ssd-.+"
  }

```

# Rest API

## Get / Update deployment config

### fetch current deployment configuration

```
curl http://127.0.0.1:8080/v1/config/deployment-config.conf
```

### update current deployment configuration

```
# where deployment-config.conf is a file residing on the local filesystem:

curl -X PUT http://127.0.0.1:8080/v1/config/deployment-config.conf -d @deployment-config.conf
```

Please note that access to this endpoint contains an inherent security risk, as it is possible to specify the
docker_image used to deploy ceph, and therefore could be used to gain access to the ceph secrets, and run arbitrary
a specified container with host networking privileges. Protect the endpoint, accordingly.

## list tasks

```
curl http://127.0.0.1:8080/v1/jobs
```

## Update job goal

```
# pause a job
curl -X PUT http://127.0.0.1:8080/v1/jobs/aa76c126-16e8-4b43-a861-663332657f61/paused

# resume a job
curl -X PUT http://127.0.0.1:8080/v1/jobs/aa76c126-16e8-4b43-a861-663332657f61/running
```


# Updating configuration

Config updates to `(ceph)/ceph.conf` happen as follows:

- Increases to # of nodes to deploy are picked up immediately.
- Deployment resource config is picked up immediately, but USED FOR LAUNCHING new jobs only. Currently there is no
  support for changing resources for a job.
- Ceph configuration is picked up immediately, but applied as jobs are launched (IE - they are restarted).

Note that the latest configuration is deployed when a job is relaunched.
