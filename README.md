# Description

Officially supported by Redis Inc, a feature complete Redis backend that implements `Zend Cache Backend` interface and
allowing to use Redis server as cache storage. Based on [Predis](https://github.com/predis/predis) PHP client for Redis 
database. Tags are fully supported except of `TwoLevels` cache and is a great use for single server instance or Redis 
Enterprise cluster. Suits for any Zend Framework projects including all versions of Magento.

## Getting started

```bash
$ composer require redis-labs-modules/predis-magento-cache
```

## Features
- Fully supported tags with implementation based on Redis data structures "set" and "hash" for easy management and the best performance.
- Automatic key expiration performed by Redis.
- Supports redis standard socket implementations. For more information see [Connecting to Redis](https://github.com/predis/predis#connecting-to-redis).
- Supports data compression as long as one of the PHP extensions: gzip, lzf, snappy, or zlib are available.
- Uses transactions for data consistency in places where it's needed.
- Uses pipelines for best performance.
- Support replication mechanism (one master and single/multiple slaves).
- Supports configurable `auto expiry lifetime` to set custom TTL for cache records.

## Options
Available `$options` array properties.

Connection:
- `uri` - String. Specifies connection uri to single redis instance. If following is set all other connection options will be rejected. Works for any socket connections. Example: `tcp://127.0.0.1:6379`.
- `database` - Integer. Specifies database number.
- `port` - String/Integer. Specifies server port.
- `username` - String. Specifies authentication username.
- `password` - String. Specifies authentication password.
- `replication` - Array. Specifies replication configuration. More information here.

Backend:
- `automatic_cleaning_factor` - Integer (0,1). Defines if automatic cleaning should be enabled.
- `notMatchingTags` - Boolean. Defines if backend should support condition "not matching tags" applicable for ids and tags management.
- `use_lua` - Boolean. Enable/Disable executing some parts of code server-side via predefined LUA scripts for best performance. Disabled by default.
- `auto_expire_lifetime` - Integer. Defines custom auto expire lifetime.
- `auto_expire_refresh_on_load` - Boolean. Works in combination with `auto_expire_lifetime`. Enables auto expire refresh when id is loaded.
- `compress_tags` - Integer. Specifies level of compression for tags (required for some compression libraries). Default value 1.
- `compress_data` - Integer. Specifies level of compression for data (required for some compression libraries). Default value 1.
- `compression_lib` - String. Specifies compression library to use for data compression. If empty value given `gzip` will be used.
- `compress_threshold` - Integer. Specifies a threshold when data compression should be enabled. Sometimes it makes sense to enable data compression for large data sets only, so you can configure threshold, so if `strlen($data) >= $threshold` data compression will be activated. Default value is 20480 characters.

## Replication
Backend can be configured to operate in a single master / multiple slaves setup to provide better service availability.

```php
$options = [
    'replication' => [
        'driver' => 'predis',
        'connections' => [
            ['scheme' => 'tcp', 'server' => '10.0.0.1', 'port' => '6379', 'role' => 'master'],
            ['scheme' => 'tcp', 'server' => '10.0.0.2', 'port' => '6379'],
            ['scheme' => 'tcp', 'server' => '10.0.0.3', 'port' => '6379'],  
        ],
    ],
];
```

The example above has a static list of servers and relies entirely on the client's logic (`'driver' => 'predis'`), this 
way client's handle read/write operations by itself, but it is possible to rely on redis-sentinel for a more robust HA 
environment with sentinel servers acting as a source of authority for clients for service discovery. The minimum 
configuration required by the client to work with redis-sentinel:

```php
$options = [
    'replication' => [
        'driver' => 'sentinel',
        'service' => 'myservice',
        'connections' => [
            ['scheme' => 'tcp', 'server' => '10.0.0.1', 'port' => '6379', 'role' => 'master'],
            ['scheme' => 'tcp', 'server' => '10.0.0.2', 'port' => '6379'],
            ['scheme' => 'tcp', 'server' => '10.0.0.3', 'port' => '6379'],  
        ],
    ],
];
```

If the master and slave nodes are configured to require an authentication from clients, a password must be provided via 
the global parameters client option. This option can also be used to specify a different database index. 
The client options array would then look like this:

```php
$options = [
    'replication' => [
        'driver' => 'sentinel',
        'service' => 'myservice',
        'password' => 'password',
        'database' => 10,
        'connections' => [
            ['scheme' => 'tcp', 'server' => '10.0.0.1', 'port' => '6379', 'role' => 'master'],
            ['scheme' => 'tcp', 'server' => '10.0.0.2', 'port' => '6379'],
            ['scheme' => 'tcp', 'server' => '10.0.0.3', 'port' => '6379'],  
        ],
    ],
];
```

### Load balancing
By default, load balancing is enabled, so all reads executed against replicas and writes against master. It's possible to 
change it with given callback as `driver` value:

```php
$options = [
    'replication' => [
        'driver' => function() {
            $strategy = new Predis\Replication\ReplicationStrategy();
            $strategy->disableLoadBalancing();
            
            return new Predis\Connection\Replication\MasterSlaveReplication($strategy);
        },
        'service' => 'myservice',
        'password' => 'password',
        'database' => 10,
        'connections' => [
            ['scheme' => 'tcp', 'server' => '10.0.0.1', 'port' => '6379', 'role' => 'master'],
            ['scheme' => 'tcp', 'server' => '10.0.0.2', 'port' => '6379'],
            ['scheme' => 'tcp', 'server' => '10.0.0.3', 'port' => '6379'],  
        ],
    ],
];
```

## Example

```php
require_once 'vendor/autoload.php';

$options = [
    'server' => '127.0.0.1',
    'database' => '0',
    'port' => '6379',
    'password' => '',
    'compress_data' => '1',
    'compression_lib' => '',
];

$backend = new \Redis\Pmc\Cache\Backend\Redis($options);

// true
$backend->save('data', 'id', ['tag1', 'tag2']);

// "data"
$backend->load('id');

// ["tag1", "tag2"]
$backend->getTags();

// ["expire" => 1683039797, "tags" => ["tag1", "tag2"], "mtime" => "1683036197"]
$backend->getMetadatas('id');

// ["id"]
$backend->getIdsMatchingTags(['tag1', 'tag2']);

// true
$backend->remove('id');

// []
$backend->getIds();

// true
$backend->clean(Zend_Cache::CLEANING_MODE_OLD);

// []
$backend->getTags();
```
