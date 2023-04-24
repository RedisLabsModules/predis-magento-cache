<?php

namespace Redis\Pmc\Unit;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Predis\Client;
use Predis\ClientInterface;
use Redis\Pmc\Cache\Backend\FactoryInterface;
use Redis\Pmc\Cache\Backend\Redis;

final class RedisTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    /**
     * @var MockObject&FactoryInterface&MockObject
     */
    private $mockFactory;

    /**
     * @var MockObject&Client&MockObject
     */
    private $mockClient;

    protected function setUp(): void
    {
        $this->mockFactory = \Mockery::mock(FactoryInterface::class);
        $this->mockClient = \Mockery::mock(ClientInterface::class);

        $this->mockFactory
            ->shouldReceive('create')
            ->once()
            ->withAnyArgs()
            ->andReturn($this->mockClient);
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsReturnIdsArray(): void
    {
        $expectedResponse = ['key1', 'key2'];

        $this->mockClient
            ->shouldReceive('keys')
            ->withArgs([Redis::PREFIX_KEY.'*'])
            ->andReturn([Redis::PREFIX_KEY.'key1', Redis::PREFIX_KEY.'key2']);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIds());
    }
}
