<?php

namespace Redis\Pmc\Cache\Backend;

use Predis\Client;
use Predis\ClientInterface;

class ClientFactory implements FactoryInterface
{
    /**
     * Mapping for parameters to match expected configuration.
     */
    protected array $parametersMapping = [
        'server' => 'host',
    ];

    /**
     * Creates client with given configuration.
     *
     * @throws \Zend_Cache_Exception
     */
    public function create(array $options): ClientInterface
    {
        if (isset($options['uri'])) {
            return new Client($options['uri']);
        }

        if (isset($options['server'])) {
            $options = $this->mapOptions($options);

            return new Client($options);
        }

        if (isset($options['replication'])) {
            if (is_array($options['replication'])) {
                return $this->setupReplicationClient($options['replication']);
            }

            \Zend_Cache::throwException(
                'Replication configuration should be specified as array'
            );
        }

        if (isset($options['cluster'])) {
            if (is_array($options['cluster'])) {
                return $this->setupClusterClient($options['cluster']);
            }

            \Zend_Cache::throwException(
                'Cluster configuration should be specified as array'
            );
        }

        \Zend_Cache::throwException('Unknown connection type.');
    }

    /**
     * Maps user defined backend options to options and parameters expected for client.
     */
    protected function mapOptions(array $options): array
    {
        $mappedOptions = [];

        foreach ($options as $key => $value) {
            if (array_key_exists($key, $this->parametersMapping)) {
                $mappedOptions[$this->parametersMapping[$key]] = $value;
            } else {
                $mappedOptions[$key] = $value;
            }
        }

        return $mappedOptions;
    }

    /**
     * Setup replication client according to given configuration.
     */
    protected function setupReplicationClient(array $replicationConfiguration): ClientInterface
    {
        $parameters = [];
        $options = [
            'replication' => $replicationConfiguration['driver'] ?? 'sentinel',
            'service' => $replicationConfiguration['service'] ?? '',
            'parameters' => [
                'password' => $replicationConfiguration['password'] ?? '',
                'database' => $replicationConfiguration['database'] ?? 0,
            ],
        ];

        foreach ($replicationConfiguration['connections'] as $connection) {
            $connection = $this->mapOptions($connection);
            $scheme = $connection['scheme'] ?? 'tcp';
            $port = $connection['port'] ?? 6379;
            $uri = $scheme.'://'.$connection['host'].':'.$port;

            if (isset($connection['role'])) {
                $uri .= "?role={$connection['role']}";
            }

            $parameters[] = $uri;
        }

        return new Client($parameters, $options);
    }

    /**
     * Setup cluster client according to given configuration.
     */
    protected function setupClusterClient(array $clusterConfiguration): ClientInterface
    {
        $parameters = [];
        $options = [
            'cluster' => $clusterConfiguration['driver'] ?? 'redis',
            'parameters' => [
                'password' => $clusterConfiguration['password'] ?? '',
                'database' => $clusterConfiguration['database'] ?? 0,
            ],
        ];

        foreach ($clusterConfiguration['connections'] as $connection) {
            $connection = $this->mapOptions($connection);
            $scheme = $connection['scheme'] ?? 'tcp';
            $port = $connection['port'] ?? 6379;
            $uri = $scheme.'://'.$connection['host'].':'.$port;

            if (isset($connection['alias'])) {
                $uri .= "?alias={$connection['alias']}";
            }

            $parameters[] = $uri;
        }

        return new Client($parameters, $options);
    }
}
