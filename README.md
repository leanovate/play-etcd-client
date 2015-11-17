# play-etcd-client

Build status: [![Build Status](https://travis-ci.org/leanovate/play-etcd-client.svg?branch=master)](https://travis-ci.org/leanovate/play-etcd-client) [![codecov.io](https://codecov.io/github/leanovate/play-etcd-client/coverage.svg?branch=master)](https://codecov.io/github/leanovate/play-etcd-client?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.leanovate/play-etcd-client_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.leanovate/play-etcd-client_2.11)

Simple library to use etcd in a play application.

## Usage

The current version is supposed to run with Play 2.4 or higher.

Add the following dependency to your project

```
libraryDependencies += "de.leanovate" %% "play-etcd-client" % "2.4.0"
```

The library has two main classes:

* `de.leanovate.play.etcd.EtcdClient`
  * Encapsulates low level interactions with the etcd cluster.
  * `getNode`: Get value or directory node
  * `createValue`: Create a value node in a directory (with auto-generated key)
  * `updateValue`: Create or update a value node
  * `deleteValue`: Delete a value node
  * `updateDir`: Create or update a directory node
  * `deleteDir`: Delete directory node
* `de.leanovate.play.etcd.EtcdOperation`
  * Implements higher level usecases one might realize with an etcd cluster.
  * `getValues`: Convenient method to get values from value or directory nodes
  * `transformValue` / `tryTransformValue`: Perform an atomic transformation on a value node
  * `enqueueValue` / `dequeueValue`: Use a directory node as queue
  * `lock` / `tryLock`: Perform a cluster-wide synchronization of code block

## License

[MIT Licence](http://opensource.org/licenses/MIT)
