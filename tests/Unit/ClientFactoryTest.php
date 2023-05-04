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
                        ],
                    ],
                ]],
            ],
        ];
    }
}
