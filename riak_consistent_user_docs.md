# Riak 2.0: Strong Consistency

Riak 2.0 now provides support for strongly consistent operations,
allowing you to choose between eventual consistency and strong
consistency on a per-key basis.

The consistency mode for a given key is determined by the configuration
of the bucket type the key is stored under.

For usage details, see [Using Strong
Consistency](http://docs.basho.com/riak/2.0.0/dev/advanced/strong-consistency)

Note: In addition to setting `strong_consistency = on` as discussed in
the above link, a cluster must also have at least 3 nodes before the
consistency subsystem is activated.

## What does "strong consistency" mean?

The phrase "strong consistency" is often used in different contexts to
mean different things.

For Riak, "strong consistency" refers to single-key atomic operations.

Unlike eventually consistent operations, these operations never
generate siblings and always operate on the most recently written
value of a key.

There are four types of atomic operations provided in Riak 2.0:
* get
* conditional put
* conditional modify
* delete

There is not a new client API for consistent operations. Instead, the
existing client API has different semantics when used against
consistent keys.

`get` operations against a consistent key work as they do against
normal keys, except that the operation is guaranteed to return the
most recently written value in all cases. And the operation will never
return a value with siblings.

`put` operations against a consistent key map either to an atomic
`conditional put` or an atomic `conditional modify` depending on if
the object being written was freshly created by the client or is a
modification of an object previously fetched from Riak.

A `conditional put` is a "put if does not exist" operation. If the key
already exists, the operation fails. Otherwise, if the key was never
written or has been deleted, the operation succeeds.

<a name="conditional-modify"></a>
A `conditional modify` is a "compare and swap (CAS)" operation that
succeeds only if the value has not changed since it was previously
read. As a user, you should envision the `conditional modify` as the
entire read, modify, write cycle. Specifically:

1. A = get(key)
2. B = modify A as you see fit
3. put(B)

The `put` at step 3 will be treated as a `conditional modify` and will
succeed if the key has not changed since it was read at step
1. Otherwise, the operation will fail and you must start the entire
process over from step 1. Or alternatively, give up on the operation.
Whatever is more appropriate for your application.

Note: consistent operations (gets, puts, deletes) ignore the majority of
per-request options provided by the normal API. Only the `timeout` and
`return_body` (in the case of puts) options are handled.

Note: In a future version, Riak will also support strongly consistent
[Data Types](http://docs.basho.com/riak/2.0.0/dev/using/data-types)
thus providing atomic increment/decrement, atomic set operations, etc.

## Consistency Design / Tradeoffs

Riak implements strong consistency by sharding the key space across multiple
independent consensus groups (called ensembles) that use a variant of the
multi-paxos protocol.

What does that mean for you, the user?

First, strongly consistent operations operate against a quorum of
primary replicas at all times. The concept of tuning `R`, `W`, `PR`,
and `PW` on a per-request basis as with normal Riak operations has
no analogy for consistent operations.

As such, at least a quorum of primary replicas must be online and
reachable for an operation to succeed. Thus, consistent operations are
less highly available than eventually consistent operations.

As a specific example, during a network partition only one side of
the partition will be able to perform consistent operations, while
both sides are able to perform eventually consistent operations.  This
is the canonical AP vs CP tradeoff embodied in the CAP theorem.

Nevertheless, consistent operations still provide a great deal of fault
tolerance. Up to a minority of replicas per ensemble can be offline, faulty, or
unreachable and consistent operations will continue to operate without issue.

Second, the size of each consensus group is a function of the `N` value.
By default Riak uses `N=3`, so each consensus group will have 3 peers.
However, a bucket type with `N=5` would result in a separate set of
consensus groups with 5 peers. There is an independent set of
consensus groups for each distinct `N` value.

The larger the consensus group, the more fault tolerant the group is.
With 3 replicas, a quorum majority requires 2/3 nodes online; while as
for 5 replicas, only 3/5 nodes need to be online. It is recommended to
use `N=5` or `N=7` for strongly consistent data.

Third, each consensus group automatically organizes itself into a
leader and a set of followers that follower that leader. All
operations that target a given consensus group are serialized through
the leader, thus consistent operations are typically slower than
normal Riak operations.  The exact performance differences depend
greatly on the workload.

Fourth, as mentioned above Riak shards the key space across multiple
consensus groups. The number of consensus groups scales with the
cluster ring size. Thus, increasing the ring size results in more fine
grained sharding, more independent consensus groups, and more
concurrency/higher throughput. Thus, while consistent operations are
slower than normal Riak operations, consistent operations retain
Riak's scalability properties: adding more nodes to a Riak cluster
with an appropriate ring size results in higher cluster wide
throughput.

## Round Trips

The latency of a request is impacted by the number of quorum round
trips that must be performed to satisfy the request.

For eventually consistent data, a coordinating node can always satisify
a request with at most 1 round trip.

For strongly consistent data, the leader can in the common case
also satisify a request with at most 1 round trip; however, there
are other cases that can require additional round trips.

The following table summarizes the situation:

|                    | round trips          |
|--------------------|----------------------|
| read (leased)      | 0                    |
| read (common)      | 1                    |
| read (worst)       | 1 (read) + 1 (write) |
| write (common)     | 1                    |
| write (worst)      | 1 (read) + 1 (write) |

The worst case scenarios are the most interesting cases. For both read
and write the worst case is 2 round trips with one being a quorum
read and the other a quorum write. This scenario occurs the very first
time a given key is accessed after a new leader has been elected. This
occurs because a new leader must refresh all encountered keys to the
current consensus epoch. This refresh entails the leader doing a
quorum read to get the most recent known value and then rewriting
that value back to the quorum with the latest epoch information.

There are two important details to remember. First, this worst case
behavior only occurs the very first time a value is accessed after a
leader change. Second, a refresh occurs for both reads and writes. Thus,
in the worst case, a read operation may internally trigger a write.

In a cluster with a stable leader, the common case is the same as normal
eventually consistent operations: 1 round trip at most. Furthermore, when
using leader leases, a read can be handled directly by the leader in the
common case completely avoiding all quorum round trips.

## Leader Leases

The consensus subsystem in Riak is capable of using leader leases that
allow a leader to directly service requests without talking to any followers.
To support leader leases, Riak uses a stable leadership protocol that
ensures that a new leader can not be elected until after followers have
abandoned the previous leader.

The correctness of this approach is predicated on the assumption that
a leader will always detect that it is unable to extend its lease
before its followers timeout and abandon it.

The actual lease protocol works by using independent monotonic timers
on each peer. There is no requirement that the peers have the same
time, simply that the relative timers on each peer fire at a rate that
approximates real time. The implementation uses a combination of
[Erlang time correction](http://www.erlang.org/doc/apps/erts/time_correction.html)
as well as the OS-provided monotonic clock to double check lease validity.

Likewise, the default settings set the follower timeout to four times the lease
duration, making the protocol safe even in the scenario where follower clocks
run up to four times as fast as the leader's clock.

Nevertheless, if you do not trust this protocol and prefer to not use leader
leases, you can change the `trust_lease` ([details](#trust-lease)) setting
to `false` to ignore leader leases and always perform quorum reads.

## Data Corruption / Byzantine Faults

The subsystem in Riak that is responsible for enabling strongly
consistent operations is designed to detect and recover from a number
of data corruption scenarios.

However, this detection/recovery comes at a price: there are scenarios
in which Riak requires more than a simple quorum majority to be available
in order to handle requests.

Understanding the specifics of this design is important, since Riak
allows you to change the configuration to prefer availability over
safety if desired.

### The Problem

The consistency subsystem in Riak ensures that data is correctly and
consistently committed to at least a majority of replicas for a given
successful write.

However, committing data successfully at write time does not by itself
guarantee the integrity of data in the future.

Consider a three node cluster with nodes 1, 2, and 3:

* The value A is successfully written to nodes 1 and 2.
* At some point, node 1 goes offline.
* While node 1 is offline, node 2 has disk corruption and forgets A.
* You attempt to read A.
* A quorum of nodes (2 and 3) respond with `notfound`. Because there is a majority.
  the read succeeds and returns `notfound`.
* This is incorrect. You previously wrote A, `notfound` is an invalid response.

The underlying problem is that node 2 was assumed to have a valid copy
for A, but in reality node 2 had no recollection of A due to disk
corruption.

This is a case of undetected data corruption.

Had node 2 been able to detect that A was corrupted (eg. from a CRC
check), it could have correctly failed the request. But, it was not
able to do so.

While most disk corruption is detectable, undetected corruption is a
very real threat. Often times this arises from corruption of backend
metadata rather than data blocks, resulting in the backend simply
losing track of a range of stored keys.

Another possibility is data traveling back in time due to a partial
restore from a backup. For example, a user might restore the LevelDB
data from a backup while forgetting to also restore the state used by
the consensus system (which is stored in a different directory).
Thus, the Riak node would come online believing it was at consensus
epoch Y yet it would have the data from epoch X, where X < Y.

Riak is designed to address all of these issues: detected data corruption,
undetected data corruption, and data traveling back in time.

### Integrity Checking

The consistency subsystem handles this issue by having all consensus
peers maintain a persistent [Merkle tree](http://en.wikipedia.org/wiki/Merkle_tree)
for all consistent data stored by that peer.

If a write succeeds to a quorum of replicas, a second request is sent
out to update the Merkle tree on at least a quorum of replicas. Thus,
a successful write is proof of both a majority of replicas containing
the object itself as well as a majority containing object metadata in
their local Merkle trees.

Anytime the leader reads a consistent object from the backend, it also
reads the appropriate metadata from the Merkle tree and verifies that
the object and metadata match. If you were to assume the Merkle tree
is always valid, then this approach can always determine if an object
is valid or not.

Riak is designed to guarantee that the leader's Merkle tree is in fact
always valid provided that less than a majority of peers for a given
consensus group suffer permanent data loss.

However, to provide this guarantee it is sometimes necessary for
consensus peers to perform a Merkle key exchange with a majority of
trusted peers. By default, when a node reboots all Merkle trees on
that node are considered untrusted, and therefore the peer must
exchange with a majority of peers not including itself. In practice,
this can lead to situations where more than a simple majority is
required for Riak to service requests.

For example, in a 3 node consensus group, if node 1 is offline then
nodes 2 and 3 can make quorum and keep servicing requests. However, if
node 2 were to restart, it would come back online in an untrusted
state and would not be able to become trusted again until after
syncing with both nodes 1 and 3. Thus, the consensus group would
remain unavailable until node 1 came back online.

For this reason it is suggested that 5 or 7 node consensus groups
are used for consistent data rather than 3 node groups.

Alternatively, you can force Riak to assume Merkle trees are valid
after a reboot by changing the `tree_validation`
([details](#tree-validation)) setting to `false`. This allows Riak to
still detect and repair corruption for backend K/V data, but makes
Riak unable to protect against corruption of the Merkle trees
themselves.


## Operation / Status

The new `riak-admin ensemble-status` command can be used to look at the
state of the consistency subsystem. If your consistent operations are all
failing, this is a good place to check to see what is going on.

For example, if you forgot to enable strong consistency in `riak.conf`,
then `riak-admin ensemble-status` will make it clear what the problem is:

```
============================== Consensus System ===============================
Enabled:     false
Active:      false
Ring Ready:  true
Validation:  strong (trusted majority required)
Metadata:    best-effort replication (asynchronous)

Note: The consensus subsystem is not enabled.

================================== Ensembles ==================================
There are no active ensembles.
```

In the common case when all is working, you should see an output similar to
the following:

```
============================== Consensus System ===============================
Enabled:     true
Active:      true
Ring Ready:  true
Validation:  strong (trusted majority required)
Metadata:    best-effort replication (asynchronous)

================================== Ensembles ==================================
 Ensemble     Quorum        Nodes      Leader
-------------------------------------------------------------------------------
   root       4 / 4         4 / 4      riak@riak1
    2         3 / 3         3 / 3      riak@riak2
    3         3 / 3         3 / 3      riak@riak4
    4         3 / 3         3 / 3      riak@riak1
    5         3 / 3         3 / 3      riak@riak2
    6         3 / 3         3 / 3      riak@riak2
    7         3 / 3         3 / 3      riak@riak4
    8         3 / 3         3 / 3      riak@riak4
```

This output tells you that the consensus system is both enabled and active, as well
as lists details about all known consensus groups (ensembles). The `Quorum` column
lists the number of ensemble peers that are either leading or following; the
`Nodes` column shows the number of online nodes; and the `Leader` column shows the
current leader for each ensemble.

The `Validation` section at the top determines if strong Merkle tree validation
is enabled or not. This is determined based on the `tree_validation`
([details](#tree-validation)) setting. There are two outputs that will be shown
in `ensemble-status`: `strong (trusted majority required)` and `weak (simple
majority required)`. See [Integrity Checking](#integrity-checking) for more
details.

The `Metadata` section shows if follower Merkle trees are updated synchronously
or asynchronously. This is determined by the `synchronous_tree_updates`
([details](#synchronous-tree-updates)) setting. The two possible outputs in
`ensemble-status` are: `best-effort replication (asynchronous)` and `guaranteed
replication (synchronous)`.

Details about specific ensembles can be seen by specifying the number from the
`Ensemble` column. For example:

```
riak-admin ensemble-status 2
```

```
================================= Ensemble #2 =================================
Id:           {kv,0,3}
Leader:       riak@riak2
Leader ready: true

==================================== Peers ====================================
 Peer  Status     Trusted          Epoch         Node
-------------------------------------------------------------------------------
  1    following    yes             1            riak@riak1
  2     leading     yes             1            riak@riak2
  3    following    yes             1            riak@riak3
```

The `Id` field lists the internal ensemble id which in this case corresponds
to the Riak K/V ensemble responsible for `N=3` data owned by the ring preflist 0.

The `Leader` and `Leader ready` are self explanatory. If the leader is not yet
ready, requests against that ensemble will fail.

The `Status` column lists the state of the current peer. If peers are in a state
other than `leading` or `following` for a long period of time, that generally
means the ensemble is not able to make quorum and service requests. Typically,
this occurs when there are not enough online and reachable peers.

The `Trusted` column shows if the peer's Merkle tree is currently considered
trusted or not. Remember that when `tree_validation` ([details](#tree-validation))
is enabled a majority of trusted peers is necessary to bring untrusted peers up
to date.

The `Epoch` column lists the current consensus epoch. The epoch is incremented
any time the leader changes. As mentioned in [Round Trips](#round-trips) this
will force a refresh for each key encountered for the first time after the epoch
changed.

Note: If `ensemble-status` ever shows a number after the leader, such as:

```
============================== Consensus System ===============================
Enabled:     true
Active:      true
Ring Ready:  true
Validation:  strong (trusted majority required)
Metadata:    best-effort replication (asynchronous)

================================== Ensembles ==================================
 Ensemble     Quorum        Nodes      Leader
-------------------------------------------------------------------------------
   root       3 / 3         3 / 3      riak@riak1
    2         3 / 3         2 / 2      riak@riak2 (2)
    3         3 / 3         2 / 2      riak@riak2 (2)
    4         3 / 3         3 / 3      riak@riak2
    5         3 / 3         3 / 3      riak@riak2
    6         3 / 3         2 / 2      riak@riak2 (2)
    7         3 / 3         2 / 2      riak@riak2 (2)
    8         3 / 3         3 / 3      riak@riak2
```

this means that an ensemble has multiple peers on the same physical node. This
can occur when a cluster does not have enough members for Riak to guarantee
placement of all replicas on distinct nodes. This should be corrected by adding
more nodes, since having multiple replicas on the same physical node leads to
lower availability and durability guarantees.

The actual significance of the number itself is to identify which of the peers
on the node is the actual leader. For example, ensemble 2 lists `riak@riak2 (2)`
as the current leader. If you look at the specific details for ensemble 2:

```
================================= Ensemble #2 =================================
Id:           {kv,0,3}
Leader:       riak@riak2 (2)
Leader ready: true

==================================== Peers ====================================
 Peer  Status     Trusted          Epoch         Node
-------------------------------------------------------------------------------
  1    following    yes             1            riak@riak1
  2     leading     yes             1            riak@riak2
  3    following    yes             1            riak@riak2
```

you will see that it is peer 2 that is the current leader, not peer 3.

### Stats

Consistent operations increment the same `vnode_get_` and `vnode_put_` stats
as normal Riak operations. However, they do not increment any of the `node_fsm_`
stats. Instead, there is a separate set of stats that correspond to the equivalent
stats for eventually consistent operations:

```
consistent_gets
consistent_gets_total
consistent_get_objsize_mean
consistent_get_objsize_median
consistent_get_objsize_95
consistent_get_objsize_99
consistent_get_objsize_100
consistent_get_time_mean
consistent_get_time_median
consistent_get_time_95
consistent_get_time_99
consistent_get_time_100

consistent_puts
consistent_puts_total
consistent_put_objsize_mean
consistent_put_objsize_median
consistent_put_objsize_95
consistent_put_objsize_99
consistent_put_objsize_100
consistent_put_time_mean
consistent_put_time_median
consistent_put_time_95
consistent_put_time_99
consistent_put_time_100
```

## Known Issues / Limitations

### Consistent deletes do not clear tombstones

Consistent deletes are implemented as writes of a special tombstone value similar to
eventually consistent deletes. However, garbage collection of consistent tombstones
is currently not implemented. Thus, deleted keys still take up space, on the order
of 50 - 100 bytes + key size.

### Consistent reads of never-written keys creates tombstones

Performing a read against a consistent key that a majority claims does not
exist will cause a tombstone to be written. This is necessary to handle certain
corner cases regarding offline/unreachable replicas that contain partially written
data that needs to be rolled back in the future.

This problem is further exacerbated due to the previously mentioned issue that
tombstones are not garbage collected.

### Consistent keys and key listing

In Riak, key list operations do not filter out tombstones. For eventually
consistent keys this is rarely an issue. However, this is a problem for
consistent keys due to the aforementioned tombstone issues.

### Consistent keys and secondary indexes

Consistent operations do not support secondary indexes at this time. Any
supplied index metadata will be silently ignored. This is identical to
how Riak ignores index metadata when using a backend that does not support
secondary indexes (eg. Bitcask).

### Consistent keys and search

Consistent keys can be indexed by the [Search](http://docs.basho.com/riak/2.0.0/dev/using/search)
subsystem. The primary caveat is that while the key/value objects are strongly consistent, the
search index remains eventually consistent. Just-written data may not immediately appear in the
search index. Furthermore, failed writes that the consistency subsystem will rollback on a future
read may appear in the search index temporarily.

Using search with consistent keys works best for use cases where the application will read all the keys
returned by the search query, rather than use the query results directly. The consistent read will
correctly return the most recently committed value against which your application can double check if
it is still a valid result for the given query.

### Consistent keys and multi data center replication

Consistent keys are ***not*** replicated between clusters that use Riak Enterprise's
replication feature. The reason for this is that the multi-DC replication feature uses
eventually consistent replication. Having data that is strongly consistent within a
cluster, but eventually consistent between clusters is difficult to reason about and
build successful applications on.

Instead, the goal is to extend Riak in a future version to support strongly consistent
replication of consistent keys across multiple clusters / data centers.

### Conditional operations and client libraries

Currently, the majority of Riak client libraries convert errors returned by Riak into
a generic exception with a message derived from the returned server-side error message.
Traditionally this has been fine, since error conditions are exception in normal Riak
usage.

However, the conditional nature of consistent puts results in certain errors being
common case. For example, it is expected behavior for a conditional modify to fail if
the object was changed concurrently. At present, most clients will convert this
failure into an exception no different than other error conditions. As a user, you
will need to catch these exceptions and look at the embedded server-side error message
to see if the issue was a conditional precondition failure (and then retry your
modification if desired), or an actual unexpected exceptional condition.

Also, remember that if it is a precondition failure, you must re-perform the entire
get / modify / put cycle. Simply re-issuing the put of the previously modified value
will continue to fail. See [conditional modify](#conditional-modify) for more
details. Likewise, client retry logic should be disabled for writes against consistent
keys, since the retry logic may try to reissue the write without realizing that
it will always continue to fail.

Note: A future Riak 2.0 point release will address this issue, changing the server API
and client functionality to better handle this condition without requiring you to manually
parse the error message.

(TODO: Add actual client code examples here for Java, Ruby, Python, etc)

## Recommendations

### Use 5-way or 7-way replication for consistent keys

As mentioned above, ensembles remain available as long as a majority of ensemble peers
are online and reachable. Thus, the larger the ensemble the more failures the ensemble
can tolerate and still service requests.

| replicas | tolerated failures |
|----------|--------------------|
| 3        | 1                  |
| 5        | 2                  |
| 7        | 3                  |
| 9        | 4                  |

While using 3 replicas does allow you to tolerate 1 unexpected fault, it puts a lot of
pressure on resolving the fault as soon as possible to ensure additional faults do not
occur. Thus, using 5 or 7 replicas is generally better from an operational perspective.

Using 5 replicas allows you to tolerate 2 unexpected faults, or 1 unexpected fault while
intentionally having 1 node offline (eg. for service/upgrade/administrative reasons).

With 7 replicas you can tolerate 3 unexpected faults, 2 unexpected faults while having
1 intentionally-offline node, or 1 unexpected fault while having 2 intentionally-offline
nodes.

Also remember that Merkle tree validation temporarily treats rebooted nodes the
same as offline nodes. Thus, when tree validation is enabled (which is the
default), you need more online nodes to remain available.

To make the impact of tree validation clear, the following table shows how many
nodes can fail or be intentionally-offline while you simultaneously restart X
nodes for different values of X.

| replicas | 1 restarted | 2 restarted | 3 restarted | 4 restarted |
|----------|-------------|-------------|-------------|-------------|
| 3        | 0           | 0           | 0           | 0           |
| 5        | 1           | 0           | 0           | 0           |
| 7        | 2           | 1           | 0           | 0           |
| 9        | 3           | 2           | 1           | 0           |

As such, it is even more important when using tree validation to use 5 or 7
replicas. Using 3 replicas with tree validation makes it impossible to tolerate
a node rebooting while another node is intentionally or unintentionally offline.

If you must use 3 replicas, consider turning off `tree_validation`
([details](#tree-validation)), However, doing so reduces the ability for Riak
to detect certain Byzantine faults as discussed in [Integrity
Checking](#integrity-checking).

Using 5 or 7 replicas for consistent data is therefore recommended. This is
determined by the `n_val` setting on the relevant bucket type. For example, the
following creates a consistent bucket types with both 5 and 7 replicas respectively:

```
riak-admin bucket-type create consistent5 '{"props": {"consistent": true, "n_val": 5}}'
riak-admin bucket-type create consistent7 '{"props": {"consistent": true, "n_val": 7}}'
```

Note: Riak has a setting called `target_n_val` that ***must*** be set higher then the
largest `n_val` used on any bucket/bucket type. If this is not true, then Riak cannot
guarantee all replicas are placed on distinct nodes. Riak ships with `target_n_val`
set to `4` by default. If you are going to use consistent bucket types with 5 or 7
replicas, you ***must*** increase the `target_n_val`.

As of Riak 2.0 RC1, setting `target_n_val` is not possible in `riak.conf` but must be
set in `advanced.config` as follows:

```
{riak_core, [{target_n_val, 5}]}.
````

More details about `target_n_val` can be found in the Riak 1.4
[configuration docs](http://docs.basho.com/riak/1.4.9/ops/advanced/configs/configuration-files)
(the setting is currently missing from the newer `riak.conf` focused 2.0 docs).

Note: when using 5 replicas, it is recommended to use a Riak cluster containing at least 8
nodes. Likewise, when using 7 replicas a Riak cluster using at least 10 and preferably 12
or more nodes is recommended.

### Reboot nodes one at a time

Rebooting too many nodes that belong to the same ensemble can force the ensemble
to lose quorum and be unable to service requests. It is therefore recommended that
you take care when rebooting nodes. Preferably rebooting nodes 1 at a time and
waiting until the rebooted node has rejoined all ensembles before continuing to
the next. The `riak-admin ensemble-status` command can be used to check the state
of ensembles.

### Limit number of intentionally offline nodes

In the same vein as rebooting nodes carefully to maintain quorum, it is also
recommended to limit the number of intentionally offline nodes that share ensembles.

### Use larger clusters for greater fault tolerance

As a Riak cluster grows, ensembles are spread out across more nodes, resulting
in fewer ensembles per node and fewer shared ensembles between any given pair
of nodes. Therefore, the impact of a node failure is diminished as the cluster
size grows.

In a 3 node cluster with `N=3` ensembles, a single node failing counts as a
failure against all ensembles. Any additional node failing will result in a
majority of failures against all ensembles, thus rendering the entire key space
unavailable.

However, in a 50-node cluster with `N=5` ensembles, each individual node is
involved in only 10% of the total number of ensembles. Thus a single node
failing counts as a failure against only 10% of the ensembles. Since
these ensembles have 5 replicas they would require 2 additional failures
to become unavailable. However, it is highly unlikely that 2 additional nodes
will fail that all impact the exact same ensembles. Even then, in this
scenario only those particular ensembles would become unavailable -- affecting
only 10% of the key space.

## Configuration

The following configuration parameters are not exposed through `riak.conf` but
instead must be set through `advanced.config`. All of these settings are part
of the `riak_ensemble` subsystem in Riak, so the proper `advanced.config` usage
looks like:

```
{riak_ensemble, [{trust_lease, true},
                 {peer_workers, 1}]}.
```

---

| <a name="trust-lease"></a> |                    | default            |
|----------------------------|--------------------|--------------------|
| trust_lease                | `true` or `false`  | `true`             |

The `trust_lease` setting determines if leader leases are used to optimize reads. When
`true`, a leader with a valid lease will handle the read directly without contacting
any followers. When `false`, the leader will always contact followers. For more
information see: [Leader Leases](#leader-leases).

---

|                    |                    | default            |
|--------------------|--------------------|--------------------|
| peer_get_timeout   | milliseconds       | 60000              |

The `peer_get_timeout` setting determines the timeout used internally for reading
consistent data. This setting must be greater than the highest request timeout used
by your application.

---

|                    |                    | default            |
|--------------------|--------------------|--------------------|
| peer_put_timeout   | milliseconds       | 60000              |

The `peer_put_timeout` setting determines the timeout used internally for writing
consistent data. This setting must be greater than the highest request timeout used
by your application.

---

|                    |                    | default            |
|--------------------|--------------------|--------------------|
| peer_workers       | integer            | 1                  |

The `peer_workers` setting determines the number of concurrent workers used by
the leader to service requests. Increasing this setting may increase performance
depending on the workload.

---

| <a name="tree-validation"></a> |                    | default            |
|--------------------            |--------------------|--------------------|
| tree_validation                | `true` or `false`  | `true`             |

The `tree_validation` setting determines if Riak considers peer Merkle trees trusted or not
after a node restart. When validation is enabled (the default), Riak does not trust peer
trees after a restart, but instead requires the peer to sync with a trusted majority. This
is the safest option, as it protects Riak against undetected corruption of the Merkle tree.
However, this mode reduces Riak availability since it can sometimes requires more than
a simple majority of nodes to be online and reachable. See [Integrity Checking](#integrity-checking)
for more details.

---

| <a name="synchronous-tree-updates"></a> |                    | default            |
|-----------------------------------------|--------------------|--------------------|
| synchronous_tree_updates                | `true` or `false`  | `false`            |

The `synchronous_tree_updates` setting determines if the metadata updates to follower Merkle
trees are handled synchronously or not. When set to `true`, Riak requires 2 quorum roundtrips
to occur before replying back to the client: the first quorum request to write the actual
object and the second to write the Merkle tree metadata. When set to `false`, Riak will respond
back to the client after the first roundtrip, letting the metadata update happen
asynchronously. To be clear, the leader always updates its local Merkle tree before responding
to the client -- this setting solely affects the metadata writes sent to followers.

In theory, asynchronous updates are unsafe. If the leader crashes before sending the metadata
updates, and all followers that had acknowledged the object write somehow revert to the object
value immediately prior to the write request, then a future read could return the immediately
preceding value without realizing it was incorrect.

Given that this scenario is exceedingly unlikely, Riak defaults to `false` for improved performance.

                                                 
## Advanced Configuration

The following configuration must be set through `advanced.config`. All of these settings are part
of the `riak_ensemble` subsystem in Riak, so the proper `advanced.config` usage looks like:

```
{riak_ensemble, [{ensemble_tick,  500},
                 {lease_duration, 1000}]}.
```

---

|                    |               | default            |
|--------------------|---------------|--------------------|
| ensemble_tick      | milliseconds  | 500                |

The `ensemble_tick` setting determines the rate at which a leader perform its periodic
duties, including refreshing the leader lease. This setting must be lower than both the
`lease_duration` and `follower_timeout` settings.

---

|                    |                    | default                |
|--------------------|--------------------|------------------------|
| lease_duration     | milliseconds       | `ensemble_tick` * 2/3  |

The `lease_duration` setting determines how long a leader lease remains valid without
being refreshed. This setting should be higher than the `ensemble_tick` setting so that
leaders have time to refresh their leases before they timeout. This setting must also be
lower than the `follower_timeout` setting.

---

|                    |                    | default              |
|--------------------|--------------------|----------------------|
| follower_timeout   | milliseconds       | `lease_duration` * 4 |

The `follower_timeout` setting determines how long a follower waits to hear from a leader
before it abandons it. This setting must be greater than the `lease_duration` setting.

---

|                    |                    | default            |
|--------------------|--------------------|--------------------|
| alive_tokens       | integer            | 2                  |

The `alive_tokens` setting determines the number of ticks the leader will wait to
hear from its associated vnode before assuming the vnode is unhealthy and stepping
down as leader. Specifically, if the vnode does not respond to the leader before
`ensemble_tick` * `alive_tokens` milliseconds have elapsed, the leader will give
up leadership. It may be necessary to raise this setting if your Riak vnodes are
frequently stalling on slow backend reads/writes. If this setting is set too low,
it may cause slow requests to timeout earlier than the requests request timeout.

---

|                    |                    | default            |
|--------------------|--------------------|--------------------|
| storage_delay      | milliseconds       | 50                 |

The `storage_delay` setting determines how long the consensus subsystem delays syncing
to disk when performing certain metadata operations. This delay allows multiple operations
to be coalesced into a single disk write. It is not recommended that users change this setting.

---

|                    |                    | default            |     
|--------------------|--------------------|--------------------|
| storage_tick       | milliseconds       | 5000               |

The `storage_tick` setting determines how often the consensus subsystem writes data to disk
that was requested to be written asynchronously. It is not recommended that users change this
setting.
