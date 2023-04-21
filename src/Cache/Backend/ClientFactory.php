<?php

namespace Redis\Pmc\Cache\Backend;

use Predis\Client;
use Zend_Cache;
use Zend_Cache_Exception;

class ClientFactory
{
    /**
     * Mapping for parameters to match expected configuration.
     *
     */
    protected array $parametersMapping = [
        'server' => 'host',
    ];

    /**
     * Creates client with given configuration.
     *
     * @param array $options
     * @return Client
     * @throws Zend_Cache_Exception
     */
    public function create(array $options): Client
    {
        if (isset($options['uri'])) {
            return new Client($options['uri']);
        }

        if (!array_key_exists('server', $options) && !array_key_exists('cluster', $options)) {
            Zend_Cache::throwException('Redis server does not specified');
        }

        if (isset($options['server'])) {
            $options = $this->mapOptions($options);

            return new Client($options);
        }

        if (isset($options['cluster'])) {
            if (is_array($options['cluster'])) {
                return $this->setupClusterClient($options['cluster']);
            }

            Zend_Cache::throwException(
                'Cluster configuration should be specified as array'
            );
        }

        if (isset($options['replication'])) {
            if (is_array($options['replication'])) {
                return $this->setupReplicationClient($options['replication']);
            }

            Zend_Cache::throwException(
                'Replication configuration should be specified as array'
            );
        }

        Zend_Cache::throwException('Unknown connection type.');
    }

    /**
     * Maps user defined backend options to options and parameters expected for client.
     *
     * @param array $options
     * @return array
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
     * Setup cluster client according to given configuration.
     *
     * @param array $clusterConfiguration
     * @return Client
     */
    protected function setupClusterClient(array $clusterConfiguration): Client
    {
        $parameters = [];
        $options = [
            'cluster' => $clusterConfiguration['driver'] ?? 'redis',
            'parameters' => [
                'password' => $clusterConfiguration['password'] ?? '',
            ]
        ];

        foreach ($clusterConfiguration['connections'] as $connection) {
            $connection = $this->mapOptions($connection);
            $scheme = $connection['scheme'] ?? 'tcp';
            $port = $connection['port'] ?? 6379;
            $uri = $scheme . "://" . $connection['host'] . ":" . $port;
            $parameters[] = $uri;
        }

        return new Client($parameters, $options);
    }

    /**
     * Setup replication client according to given configuration.
     *
     * @param array $replicationConfiguration
     * @return Client
     */
    protected function setupReplicationClient(array $replicationConfiguration): Client
    {
        $parameters = [];
        $options = [
            'replication' => $replicationConfiguration['driver'] ?? 'sentinel',
            'service' => $replicationConfiguration['service'] ?? '',
            'parameters' => [
                'password' => $replicationConfiguration['password'] ?? '',
            ],
        ];

        foreach ($replicationConfiguration['connections'] as $connection) {
            $connection = $this->mapOptions($connection);
            $scheme = $connection['scheme'] ?? 'tcp';
            $port = $connection['port'] ?? 6379;
            $uri = $scheme . "://" . $connection['host'] . ":" . $port;

            if (isset($connection['role'])) {
                $uri .= "?role={$connection['role']}";
            }

            $parameters[] = $uri;
        }

        return new Client($parameters, $options);
    }
}
