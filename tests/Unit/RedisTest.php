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
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'*'])
            ->andReturn([Redis::PREFIX_KEY.'key1', Redis::PREFIX_KEY.'key2']);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIds());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsReturnAllIdsOnNonMatchingTagProperties(): void
    {
        $expectedResponse = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_IDS])
            ->andReturn($expectedResponse);

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIds());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetTagsReturnsAllTagsArray(): void
    {
        $expectedResponse = ['tag1', 'tag2'];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_TAGS])
            ->andReturn($expectedResponse);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getTags());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsMatchingTagsReturnsIdsMatchingGivenTags(): void
    {
        $expectedResponse = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('sinter')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.'tag1', Redis::PREFIX_TAG_IDS.'tag2']])
            ->andReturn($expectedResponse);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIdsMatchingTags(['tag1', 'tag2']));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsMatchingTagsReturnsEmptyArrayOnEmptyArrayTagsGiven(): void
    {
        $this->mockClient
            ->shouldNotReceive('sinter')
            ->withAnyArgs();

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame([], $backend->getIdsMatchingTags([]));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsNotMatchingTagsReturnsAllIdsNotMatchingTags(): void
    {
        $expectedResponse = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('sdiff')
            ->once()
            ->withArgs([[Redis::SET_IDS, Redis::PREFIX_TAG_IDS.'tag1', Redis::PREFIX_TAG_IDS.'tag2']])
            ->andReturn($expectedResponse);

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIdsNotMatchingTags(['tag1', 'tag2']));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsNotMatchingTagsReturnsAllIdsOnEmptyArrayGiven(): void
    {
        $expectedResponse = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_IDS])
            ->andReturn($expectedResponse);

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIdsNotMatchingTags([]));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsNotMatchingTagsThrowsExceptionOnMissingNotMatchingTagsOption(): void
    {
        $backend = new Redis([], $this->mockFactory);

        $this->expectException(\Zend_Cache_Exception::class);
        $this->expectExceptionMessage('notMatchingTags option is currently disabled.');

        $backend->getIdsNotMatchingTags();
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsMatchingAnyTagsReturnsIdsMatchingAnyGivenTags(): void
    {
        $tags = [];
        $expectedResponse = ['id1', 'id2'];

        for ($i = 0; $i <= 520; ++$i) {
            $tags[] = 'tag'.$i;
        }

        $chunks = array_chunk($tags, 500);
        $processedTags = array_map(static function ($chunk) {
            return array_map(static function ($tag) {
                return Redis::PREFIX_TAG_IDS.$tag;
            }, $chunk);
        }, $chunks);

        $this->mockClient
            ->shouldReceive('sunion')
            ->withArgs([$processedTags[0]])
            ->andReturn(['id1']);

        $this->mockClient
            ->shouldReceive('sunion')
            ->withArgs([$processedTags[1]])
            ->andReturn(['id2']);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getIdsMatchingAnyTags($tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetFillingPercentageReturnsOneOnNonUsedMemory(): void
    {
        $config = [
            'maxmemory' => 0,
        ];

        $this->mockClient
            ->shouldReceive('config')
            ->once()
            ->withArgs(['GET', 'maxmemory'])
            ->andReturn($config);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame(1, $backend->getFillingPercentage());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetFillingPercentageReturnsCorrectFillingPercentage(): void
    {
        $config = [
            'maxmemory' => 10,
        ];

        $this->mockClient
            ->shouldReceive('config')
            ->once()
            ->withArgs(['GET', 'maxmemory'])
            ->andReturn($config);

        $this->mockClient
            ->shouldReceive('info')
            ->once()
            ->withArgs(['Memory'])
            ->andReturn(['Memory' => ['used_memory' => 20]]);

        $expectedUsedMemory = (int) round(20 / 10 * 100);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedUsedMemory, $backend->getFillingPercentage());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetMetadatasIdReturnsCorrectIdMetadata(): void
    {
        $this->mockClient
            ->shouldReceive('hmget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS, Redis::FIELD_MTIME, Redis::FIELD_INF])
            ->andReturn(['tag1,tag2', 10000, '0']);

        $this->mockClient
            ->shouldReceive('ttl')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id'])
            ->andReturn(20);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $expectedResponse = [
            'expire' => 11111 + 20,
            'tags' => ['tag1', 'tag2'],
            'mtime' => 10000,
        ];

        $backend = new Redis([], $this->mockFactory, $mockDateTime);

        $this->assertSame($expectedResponse, $backend->getMetadatas('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetMetadatasIdReturnsCorrectIdMetadataOnNonExpireKey(): void
    {
        $this->mockClient
            ->shouldReceive('hmget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS, Redis::FIELD_MTIME, Redis::FIELD_INF])
            ->andReturn(['tag1,tag2', 10000, '1']);

        $this->mockClient
            ->shouldNotReceive('ttl')
            ->withAnyArgs()
            ->andReturn(20);

        $expectedResponse = [
            'expire' => false,
            'tags' => ['tag1', 'tag2'],
            'mtime' => 10000,
        ];

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getMetadatas('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetMetadatasIdReturnsFalseOnIncorrectMTime(): void
    {
        $this->mockClient
            ->shouldReceive('hmget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS, Redis::FIELD_MTIME, Redis::FIELD_INF])
            ->andReturn(['tag1,tag2', false, '0']);

        $backend = new Redis([], $this->mockFactory);

        $this->assertFalse($backend->getMetadatas('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testTouchAddsExtraLifetimeToGivenId(): void
    {
        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_INF])
            ->andReturn('0');

        $this->mockClient
            ->shouldReceive('ttl')
            ->once()
            ->withAnyArgs()
            ->andReturn(20);

        $this->mockClient
            ->shouldReceive('expire')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', 30])
            ->andReturnTrue();

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->touch('id', 10));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testTouchDoesNotAddsExtraLifetimeToNonExpireKey(): void
    {
        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_INF])
            ->andReturn('1');

        $this->mockClient
            ->shouldNotReceive('ttl')
            ->withAnyArgs();

        $this->mockClient
            ->shouldNotReceive('expire')
            ->withAnyArgs();

        $backend = new Redis([], $this->mockFactory);

        $this->assertFalse($backend->touch('id', 10));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetCapabilitiesReturnsCapabilitiesWithoutAutomaticCleaningFactor(): void
    {
        $expectedResponse = [
            'automatic_cleaning' => false,
            'tags' => true,
            'expired_read' => false,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true,
        ];

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getCapabilities());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetCapabilitiesReturnsCapabilitiesWithAutomaticCleaningFactor(): void
    {
        $expectedResponse = [
            'automatic_cleaning' => true,
            'tags' => true,
            'expired_read' => false,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true,
        ];

        $backend = new Redis(['automatic_cleaning_factor' => true], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getCapabilities());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsDecodedDataForGivenIdIfNotMatchingAutoExpiringPattern(): void
    {
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $compressedData = $compressionPrefix.gzcompress('word1,word2,word3', 1);

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_DATA])
            ->andReturn($compressedData);

        $this->mockClient
            ->shouldNotReceive('expire')
            ->withAnyArgs();

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame('word1,word2,word3', $backend->load('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsFalseOnMissingData(): void
    {
        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_DATA])
            ->andReturn([]);

        $backend = new Redis([], $this->mockFactory);

        $this->assertFalse($backend->load('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsDecodedDataOnDisabledAutoExpireRefreshOnLoadOption(): void
    {
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $compressedData = $compressionPrefix.gzcompress('word1,word2,word3', 1);

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_DATA])
            ->andReturn($compressedData);

        $backend = new Redis(['auto_expire_refresh_on_load' => false], $this->mockFactory);

        $this->assertSame('word1,word2,word3', $backend->load('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsDecodedDataOnAutoExpireLifetimeEqualZero(): void
    {
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $compressedData = $compressionPrefix.gzcompress('word1,word2,word3', 1);

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_DATA])
            ->andReturn($compressedData);

        $backend = new Redis(['auto_expire_lifetime' => 0], $this->mockFactory);

        $this->assertSame('word1,word2,word3', $backend->load('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsDecodedDataAndSetsExpirationOnMatchingAutoExpiringPattern(): void
    {
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $compressedData = $compressionPrefix.gzcompress('word1,word2,word3', 1);

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'REQEST_id', Redis::FIELD_DATA])
            ->andReturn($compressedData);

        $this->mockClient
            ->shouldReceive('expire')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'REQEST_id', 10000]);

        $backend = new Redis(['auto_expire_lifetime' => 10000, 'auto_expire_refresh_on_load' => true], $this->mockFactory);

        $this->assertSame('word1,word2,word3', $backend->load('REQEST_id'));
    }

    /**
     * @dataProvider testProvider
     *
     * @throws \Zend_Cache_Exception
     */
    public function testTestReturnsMTimeOfGivenId(bool|int $time, bool|int $expectedResponse): void
    {
        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_MTIME])
            ->andReturn($time);

        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->test('id'));
    }

    /**
     * @dataProvider getLifetimeProvider
     * @throws \Zend_Cache_Exception
     */
    public function testGetLifetimeReturnsFalseOnFalseSpecificLifetime($lifetime, int $expectedResponse): void
    {
        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getLifetime($lifetime));
    }

    public function testProvider(): array
    {
        return [
            'with integer time' => [10000, 10000],
            'with false time' => [false, false],
        ];
    }

    public function getLifetimeProvider(): array
    {
        return [
            'with string false' => ['false', 3600],
            'with boolean false' => [false, 3600],
            'with integer' => [10000, 10000],
        ];
    }
}
