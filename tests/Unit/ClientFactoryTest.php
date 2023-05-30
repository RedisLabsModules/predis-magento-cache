<?php

namespace Redis\Pmc\Unit;

use PHPUnit\Framework\TestCase;
use Predis\Client;
use Redis\Pmc\Cache\Backend\ClientFactory;

class ClientFactoryTest extends TestCase
{
    /**
     * @dataProvider optionsProvider
     *
     * @throws \Zend_Cache_Exception
     */
    public function testCreateClientWithConfiguration(array $options): void
    {
        $factory = new ClientFactory();

        $this->assertInstanceOf(Client::class, $factory->create($options));
    }

    /**
     * @return void
     * @throws \Zend_Cache_Exception
     */
    public function testCreateClientThrowsExceptionOnMissingReplicationConfiguration(): void
    {
        $factory = new ClientFactory();

        $this->expectException(\Zend_Exception::class);
        $this->expectExceptionMessage('Replication configuration should be specified as array');

        $factory->create(['replication' => 'missing']);
    }

    /**
     * @return void
     * @throws \Zend_Cache_Exception
     */
    public function testCreateClientThrowsExceptionOnMissingClusterConfiguration(): void
    {
        $factory = new ClientFactory();

        $this->expectException(\Zend_Exception::class);
        $this->expectExceptionMessage('Cluster configuration should be specified as array');

        $factory->create(['cluster' => 'missing']);
    }

    /**
     * @return void
     * @throws \Zend_Cache_Exception
     */
    public function testCreateClientThrowsExceptionOnMissingConfiguration(): void
    {
        $factory = new ClientFactory();

        $this->expectException(\Zend_Exception::class);
        $this->expectExceptionMessage('Unknown connection type.');

        $factory->create([]);
    }

    public function optionsProvider(): array
    {
        return [
            'with URI' => [
                ['uri' => 'test'],
            ],
            'with server' => [
                ['server' => 'test'],
            ],
            'with replication' => [
                ['replication' => [
                    'connections' => [
                        [
                            'server' => 'test',
                            'port' => 6379,
                            'role' => 'master',
                        ],
                    ],
                ]],
            ],
            'with cluster' => [
                ['cluster' => [
                    'connections' => [
                        [
                            'server' => 'test',
                            'port' => 6379,
                            'alias' => 'first',
                        ],
                    ],
                ]],
            ],
        ];
    }
}
