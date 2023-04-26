<?php

namespace Redis\Pmc\Unit;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Predis\Client;
use Predis\ClientInterface;
use Predis\Pipeline\Pipeline;
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
     * @dataProvider functionTestProvider
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
     *
     * @throws \Zend_Cache_Exception
     */
    public function testGetLifetimeReturnsFalseOnFalseSpecificLifetime($lifetime, int $expectedResponse): void
    {
        $backend = new Redis([], $this->mockFactory);

        $this->assertSame($expectedResponse, $backend->getLifetime($lifetime));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenData(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $mockPipeline = \Mockery::mock(Pipeline::class);
        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('hset')
            ->once()
            ->withArgs(
                [
                    Redis::PREFIX_KEY.'id',
                    Redis::FIELD_DATA,
                    $compressionPrefix.gzcompress($data, 1),
                    Redis::FIELD_TAGS,
                    $compressionPrefix.gzcompress(implode(',', $tags), 1),
                    Redis::FIELD_MTIME,
                    11111,
                    Redis::FIELD_INF,
                    0,
                ]
            );

        $mockPipeline
            ->shouldReceive('expire')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', 3600]);

        $mockPipeline
            ->shouldReceive('sadd')
            ->once()
            ->withArgs([Redis::SET_TAGS, $tags]);

        foreach ($tags as $tag) {
            $mockPipeline
                ->shouldReceive('sadd')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag, 'id']);
        }

        foreach (explode(',', $oldTags) as $oldTag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$oldTag, 'id']);
        }

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1'],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataOnNonExpiringId(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $mockPipeline = \Mockery::mock(Pipeline::class);
        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('hset')
            ->once()
            ->withArgs(
                [
                    Redis::PREFIX_KEY.'id',
                    Redis::FIELD_DATA,
                    $compressionPrefix.gzcompress($data, 1),
                    Redis::FIELD_TAGS,
                    $compressionPrefix.gzcompress(implode(',', $tags), 1),
                    Redis::FIELD_MTIME,
                    11111,
                    Redis::FIELD_INF,
                    1,
                ]
            );

        $mockPipeline
            ->shouldNotReceive('expire')
            ->withAnyArgs();

        $mockPipeline
            ->shouldReceive('sadd')
            ->once()
            ->withArgs([Redis::SET_TAGS, $tags]);

        foreach ($tags as $tag) {
            $mockPipeline
                ->shouldReceive('sadd')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag, 'id']);
        }

        foreach (explode(',', $oldTags) as $oldTag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$oldTag, 'id']);
        }

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1'],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags, null));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataWithEmptyTagsGiven(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = [];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $mockPipeline = \Mockery::mock(Pipeline::class);
        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('hset')
            ->once()
            ->withArgs(
                [
                    Redis::PREFIX_KEY.'id',
                    Redis::FIELD_DATA,
                    $compressionPrefix.gzcompress($data, 1),
                    Redis::FIELD_TAGS,
                    '',
                    Redis::FIELD_MTIME,
                    11111,
                    Redis::FIELD_INF,
                    0,
                ]
            );

        $mockPipeline
            ->shouldReceive('expire')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', 3600]);

        $mockPipeline
            ->shouldNotReceive('sadd')
            ->withAnyArgs();

        foreach (explode(',', $oldTags) as $oldTag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$oldTag, 'id']);
        }

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1'],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataWithEmptyOldTags(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $oldTags = null;

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $mockPipeline = \Mockery::mock(Pipeline::class);
        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('hset')
            ->once()
            ->withArgs(
                [
                    Redis::PREFIX_KEY.'id',
                    Redis::FIELD_DATA,
                    $compressionPrefix.gzcompress($data, 1),
                    Redis::FIELD_TAGS,
                    $compressionPrefix.gzcompress(implode(',', $tags), 1),
                    Redis::FIELD_MTIME,
                    11111,
                    Redis::FIELD_INF,
                    0,
                ]
            );

        $mockPipeline
            ->shouldReceive('expire')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', 3600]);

        $mockPipeline
            ->shouldReceive('sadd')
            ->once()
            ->withArgs([Redis::SET_TAGS, $tags]);

        foreach ($tags as $tag) {
            $mockPipeline
                ->shouldReceive('sadd')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag, 'id']);
        }

        $mockPipeline
            ->shouldNotReceive('srem')
            ->withAnyArgs();

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1'],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataWithNonMatchingTagsOption(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $mockPipeline = \Mockery::mock(Pipeline::class);
        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('hset')
            ->once()
            ->withArgs(
                [
                    Redis::PREFIX_KEY.'id',
                    Redis::FIELD_DATA,
                    $compressionPrefix.gzcompress($data, 1),
                    Redis::FIELD_TAGS,
                    $compressionPrefix.gzcompress(implode(',', $tags), 1),
                    Redis::FIELD_MTIME,
                    11111,
                    Redis::FIELD_INF,
                    0,
                ]
            );

        $mockPipeline
            ->shouldReceive('expire')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', 3600]);

        $mockPipeline
            ->shouldReceive('sadd')
            ->once()
            ->withArgs([Redis::SET_TAGS, $tags]);

        foreach ($tags as $tag) {
            $mockPipeline
                ->shouldReceive('sadd')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag, 'id']);
        }

        foreach (explode(',', $oldTags) as $oldTag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$oldTag, 'id']);
        }

        $mockPipeline
            ->shouldReceive('sadd')
            ->once()
            ->withArgs([Redis::SET_IDS, 'id']);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1', 'notMatchingTags' => true],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataWithLuaScriptAndRedisVersionAbove7(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $arguments = [
            Redis::PREFIX_KEY,
            Redis::FIELD_DATA,
            Redis::FIELD_TAGS,
            Redis::FIELD_MTIME,
            Redis::FIELD_INF,
            Redis::SET_TAGS,
            Redis::PREFIX_TAG_IDS,
            Redis::SET_IDS,
            'id',
            $compressionPrefix.gzcompress($data, 1),
            $compressionPrefix.gzcompress(implode(',', $tags), 1),
            11111,
            0,
            3600,
            0,
        ];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $this->mockClient
            ->shouldReceive('info')
            ->once()
            ->withNoArgs()
            ->andReturn(['Server' => ['redis_version' => '7.0.0']]);

        $this->mockClient
            ->shouldReceive('executeRaw')
            ->once()
            ->withArgs([['FUNCTION', 'LIST', 'LIBRARYNAME', Redis::REDIS_LIB_NAME]])
            ->andReturn(
                [
                    ['0', '1', '2', '3', '4', [['0', 'save']]],
                ]
            );

        $this->mockClient
            ->shouldReceive('fcall')
            ->once()
            ->withArgs(['save', $tags, ...$arguments])
            ->andReturn($oldTags);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        foreach (explode(',', $oldTags) as $oldTag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$oldTag, 'id']);
        }

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1', 'use_lua' => true],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataWithLuaScriptAndRedisVersionBelow7(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $arguments = [
            Redis::PREFIX_KEY,
            Redis::FIELD_DATA,
            Redis::FIELD_TAGS,
            Redis::FIELD_MTIME,
            Redis::FIELD_INF,
            Redis::SET_TAGS,
            Redis::PREFIX_TAG_IDS,
            Redis::SET_IDS,
            'id',
            $compressionPrefix.gzcompress($data, 1),
            $compressionPrefix.gzcompress(implode(',', $tags), 1),
            11111,
            0,
            3600,
            0,
        ];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $this->mockClient
            ->shouldReceive('info')
            ->once()
            ->withNoArgs()
            ->andReturn(['Server' => ['redis_version' => '5.0.0']]);

        $this->mockClient
            ->shouldReceive('eval')
            ->once()
            ->withArgs([$this->getLuaScripts()['save']['code'], count($tags), ...$tags, ...$arguments])
            ->andReturn($oldTags);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        foreach (explode(',', $oldTags) as $oldTag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$oldTag, 'id']);
        }

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1', 'use_lua' => true],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     * @throws \Exception
     */
    public function testSaveCorrectlySavesGivenDataWithLuaScriptWithNoOldTags(): void
    {
        $data = 'data';
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;
        $tags = ['tag1', 'tag2'];
        $arguments = [
            Redis::PREFIX_KEY,
            Redis::FIELD_DATA,
            Redis::FIELD_TAGS,
            Redis::FIELD_MTIME,
            Redis::FIELD_INF,
            Redis::SET_TAGS,
            Redis::PREFIX_TAG_IDS,
            Redis::SET_IDS,
            'id',
            $compressionPrefix.gzcompress($data, 1),
            $compressionPrefix.gzcompress(implode(',', $tags), 1),
            11111,
            0,
            3600,
            0,
        ];
        $oldTags = 'tag3,tag4';

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn($oldTags);

        $mockDateTime = \Mockery::mock(\DateTime::class);
        $mockDateTime
            ->shouldReceive('getTimestamp')
            ->once()
            ->withNoArgs()
            ->andReturn(11111);

        $this->mockClient
            ->shouldReceive('info')
            ->once()
            ->withNoArgs()
            ->andReturn(['Server' => ['redis_version' => '5.0.0']]);

        $this->mockClient
            ->shouldReceive('eval')
            ->once()
            ->withArgs([$this->getLuaScripts()['save']['code'], count($tags), ...$tags, ...$arguments])
            ->andReturnNull();

        $this->mockClient
            ->shouldNotReceive('pipeline')
            ->withAnyArgs();

        $backend = new Redis(
            ['compression_lib' => 'gzip', 'compress_threshold' => '1', 'use_lua' => true],
            $this->mockFactory,
            $mockDateTime
        );

        $this->assertTrue($backend->save($data, 'id', $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testRemoveCorrectlyRemovesGivenId(): void
    {
        $tags = ['tag1', 'tag2'];

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn(implode(',', $tags));

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id']);

        foreach ($tags as $tag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag, 'id']);
        }

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }))
            ->andReturn([1]);

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->remove('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testRemoveCorrectlyRemovesGivenIdWithNotMatchingTagsOption(): void
    {
        $tags = ['tag1', 'tag2'];

        $this->mockClient
            ->shouldReceive('hget')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id', Redis::FIELD_TAGS])
            ->andReturn(implode(',', $tags));

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([Redis::PREFIX_KEY.'id']);

        $mockPipeline
            ->shouldReceive('srem')
            ->once()
            ->withArgs([Redis::SET_IDS, 'id']);

        foreach ($tags as $tag) {
            $mockPipeline
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag, 'id']);
        }

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }))
            ->andReturn([1]);

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertTrue($backend->remove('id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeAll(): void
    {
        $this->mockClient
            ->shouldReceive('flushdb')
            ->once()
            ->withNoArgs();

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeOld(): void
    {
        $tags = ['tag1', 'tag2'];
        $tag1Ids = ['id1', 'id2'];
        $tag2Ids = ['id3', 'id4'];
        $tag1ExpiredMembers = ['id1' => 0, 'id2' => 0];
        $tag2ExpiredMembers = ['id3' => 0, 'id4' => 0];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_TAGS])
            ->andReturn($tags);

        foreach ($tags as $tag) {
            $tagIds = $tag.'Ids';
            $tagExpiredMembers = $tag.'ExpiredMembers';
            $ids = $$tagIds;
            $expiredMembers = $$tagExpiredMembers;

            $this->mockClient
                ->shouldReceive('smembers')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag])
                ->andReturn($ids);

            foreach ($ids as $id) {
                $this->mockClient
                    ->shouldReceive('exists')
                    ->once()
                    ->withArgs([Redis::PREFIX_KEY.$id])
                    ->andReturn(0);
            }

            $mockPipeline = \Mockery::mock(Pipeline::class);

            foreach ($expiredMembers as $member => $status) {
                $mockPipeline
                    ->shouldReceive('srem')
                    ->once()
                    ->withArgs([Redis::PREFIX_TAG_IDS.$tag, $member]);
            }

            // Mocks pipeline object within a callback
            $this->mockClient
                ->shouldReceive('pipeline')
                ->once()
                ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                    $argument($mockPipeline);

                    return true;
                }));

            $this->mockClient
                ->shouldReceive('del')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag]);

            $this->mockClient
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::SET_TAGS, $tag]);
        }

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_OLD));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeOldWithNoTagMembers(): void
    {
        $tags = ['tag1', 'tag2'];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_TAGS])
            ->andReturn($tags);

        foreach ($tags as $tag) {
            $this->mockClient
                ->shouldReceive('smembers')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag])
                ->andReturn([]);

            $this->mockClient
                ->shouldReceive('del')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag]);

            $this->mockClient
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::SET_TAGS, $tag]);
        }

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_OLD));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeOldWithNotMatchingTagsOption(): void
    {
        $tags = ['tag1', 'tag2'];
        $tag1Ids = ['id1', 'id2'];
        $tag2Ids = ['id3', 'id4'];
        $tag1ExpiredMembers = ['id1' => 0, 'id2' => 0];
        $tag2ExpiredMembers = ['id3' => 0, 'id4' => 0];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_TAGS])
            ->andReturn($tags);

        foreach ($tags as $tag) {
            $tagIds = $tag.'Ids';
            $tagExpiredMembers = $tag.'ExpiredMembers';
            $ids = $$tagIds;
            $expiredMembers = $$tagExpiredMembers;

            $this->mockClient
                ->shouldReceive('smembers')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag])
                ->andReturn($ids);

            foreach ($ids as $id) {
                $this->mockClient
                    ->shouldReceive('exists')
                    ->once()
                    ->withArgs([Redis::PREFIX_KEY.$id])
                    ->andReturn(0);
            }

            $mockPipeline = \Mockery::mock(Pipeline::class);

            foreach ($expiredMembers as $member => $status) {
                $mockPipeline
                    ->shouldReceive('srem')
                    ->once()
                    ->withArgs([Redis::PREFIX_TAG_IDS.$tag, $member]);

                $mockPipeline
                    ->shouldReceive('srem')
                    ->once()
                    ->withArgs([Redis::SET_IDS, $member]);
            }

            // Mocks pipeline object within a callback
            $this->mockClient
                ->shouldReceive('pipeline')
                ->once()
                ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                    $argument($mockPipeline);

                    return true;
                }));

            $this->mockClient
                ->shouldReceive('del')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag]);

            $this->mockClient
                ->shouldReceive('srem')
                ->once()
                ->withArgs([Redis::SET_TAGS, $tag]);
        }

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_OLD));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeOldWithPartialyExpiredKeys(): void
    {
        $tags = ['tag1', 'tag2'];
        $tag1Ids = ['id1', 'id2'];
        $tag2Ids = ['id3', 'id4'];
        $tag1ExpiredMembers = ['id1' => 0, 'id2' => 1];
        $tag2ExpiredMembers = ['id3' => 0, 'id4' => 1];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_TAGS])
            ->andReturn($tags);

        foreach ($tags as $tag) {
            $tagIds = $tag.'Ids';
            $tagExpiredMembers = $tag.'ExpiredMembers';
            $ids = $$tagIds;
            $expiredMembers = $$tagExpiredMembers;

            $this->mockClient
                ->shouldReceive('smembers')
                ->once()
                ->withArgs([Redis::PREFIX_TAG_IDS.$tag])
                ->andReturn($ids);

            foreach ($ids as $id) {
                $expireStatus = ('id2' === $id || 'id4' === $id) ? 1 : 0;

                $this->mockClient
                    ->shouldReceive('exists')
                    ->once()
                    ->withArgs([Redis::PREFIX_KEY.$id])
                    ->andReturn($expireStatus);
            }

            $mockPipeline = \Mockery::mock(Pipeline::class);

            foreach ($expiredMembers as $member => $status) {
                if (0 === $status) {
                    $mockPipeline
                        ->shouldReceive('srem')
                        ->once()
                        ->withArgs([Redis::PREFIX_TAG_IDS.$tag, $member]);
                }
            }

            // Mocks pipeline object within a callback
            $this->mockClient
                ->shouldReceive('pipeline')
                ->once()
                ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                    $argument($mockPipeline);

                    return true;
                }));
        }

        $this->mockClient
            ->shouldNotReceive('del')
            ->withAnyArgs();

        $this->mockClient
            ->shouldNotReceive('srem')
            ->withAnyArgs();

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_OLD));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeOldWithUseLuaOption(): void
    {
        $tags = ['tag1', 'tag2'];
        $arguments = [
            Redis::PREFIX_KEY,
            Redis::SET_TAGS,
            Redis::SET_IDS,
            Redis::PREFIX_TAG_IDS,
            0,
        ];

        $this->mockClient
            ->shouldReceive('smembers')
            ->once()
            ->withArgs([Redis::SET_TAGS])
            ->andReturn($tags);

        $this->mockClient
            ->shouldReceive('info')
            ->once()
            ->withNoArgs()
            ->andReturn(['Server' => ['redis_version' => '5.0.0']]);

        $this->mockClient
            ->shouldReceive('eval')
            ->once()
            ->withArgs([$this->getLuaScripts()['collectGarbage']['code'], count($tags), ...$tags, ...$arguments]);

        $backend = new Redis(['use_lua' => true], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_OLD));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeMatchingTag(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('sinter')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([[Redis::PREFIX_KEY.$ids[0], Redis::PREFIX_KEY.$ids[1]]]);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeMatchingTagWithNotMatchingTagOption(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('sinter')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([[Redis::PREFIX_KEY.$ids[0], Redis::PREFIX_KEY.$ids[1]]]);

        $mockPipeline
            ->shouldReceive('srem')
            ->once()
            ->withArgs([Redis::SET_IDS, ...$ids]);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanDoesNotCleanOnEmptyMatchingIds(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = [];

        $this->mockClient
            ->shouldReceive('sinter')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $this->mockClient
            ->shouldNotReceive('pipeline')
            ->withAnyArgs();

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeNotMatchingTag(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('sdiff')
            ->once()
            ->withArgs([[Redis::SET_IDS, Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([[Redis::PREFIX_KEY.$ids[0], Redis::PREFIX_KEY.$ids[1]]]);

        $mockPipeline
            ->shouldReceive('srem')
            ->once()
            ->withArgs([Redis::SET_IDS, ...$ids]);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        // Mocks pipeline object within a callback
        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanDoesNotCleanOnEmptyNotMatchingIds(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = [];

        $this->mockClient
            ->shouldReceive('sdiff')
            ->once()
            ->withArgs([[Redis::SET_IDS, Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $this->mockClient
            ->shouldNotReceive('pipeline')
            ->withAnyArgs();

        $backend = new Redis(['notMatchingTags' => true], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeMatchingAnyTag(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = ['id1', 'id2'];

        $this->mockClient
            ->shouldReceive('sunion')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([[Redis::PREFIX_KEY.$ids[0], Redis::PREFIX_KEY.$ids[1]]]);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]]);

        $mockPipeline
            ->shouldReceive('srem')
            ->once()
            ->withArgs([Redis::SET_TAGS, ...$tags]);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeMatchingAnyTagWithEmptyIds(): void
    {
        $tags = ['tag1', 'tag2'];
        $ids = [];

        $this->mockClient
            ->shouldReceive('sunion')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]])
            ->andReturn($ids);

        $mockPipeline = \Mockery::mock(Pipeline::class);

        $mockPipeline
            ->shouldReceive('multi')
            ->once()
            ->withNoArgs();

        $mockPipeline
            ->shouldReceive('del')
            ->once()
            ->withArgs([[Redis::PREFIX_TAG_IDS.$tags[0], Redis::PREFIX_TAG_IDS.$tags[1]]]);

        $mockPipeline
            ->shouldReceive('srem')
            ->once()
            ->withArgs([Redis::SET_TAGS, ...$tags]);

        $mockPipeline
            ->shouldReceive('exec')
            ->once()
            ->withNoArgs();

        $this->mockClient
            ->shouldReceive('pipeline')
            ->once()
            ->with(\Mockery::on(static function ($argument) use ($mockPipeline) {
                $argument($mockPipeline);

                return true;
            }));

        $backend = new Redis([], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG, $tags));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanCorrectlyCleanInCleaningModeMatchingAnyTagWithUseLuaScriptOption(): void
    {
        $tags = ['tag1', 'tag2'];
        $arguments = [
            Redis::PREFIX_TAG_IDS,
            Redis::PREFIX_KEY,
            Redis::SET_TAGS,
            Redis::SET_IDS,
            0,
            5000,
        ];

        $this->mockClient
            ->shouldReceive('info')
            ->once()
            ->withNoArgs()
            ->andReturn(['Server' => ['redis_version' => '5.0.0']]);

        $this->mockClient
            ->shouldReceive('eval')
            ->once()
            ->withArgs([$this->getLuaScripts()['removeByMatchingAnyTags']['code'], count($tags), ...$tags, ...$arguments])
            ->andReturn('');

        $backend = new Redis(['use_lua' => true], $this->mockFactory);

        $this->assertTrue($backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG, $tags));
    }

    public function functionTestProvider(): array
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

    private function getLuaScripts(): array
    {
        return [
            'save' => [
                'code' => "local oldTags = redis.call('HGET', ARGV[1]..ARGV[9], ARGV[3]) ".
                    "redis.call('HMSET', ARGV[1]..ARGV[9], ARGV[2], ARGV[10], ARGV[3], ARGV[11], ARGV[4], ARGV[12], ARGV[5], ARGV[13]) ".
                    "if (ARGV[13] == '0') then ".
                    "redis.call('EXPIRE', ARGV[1]..ARGV[9], ARGV[14]) ".
                    'end '.
                    'if next(KEYS) ~= nil then '.
                    "redis.call('SADD', ARGV[6], unpack(KEYS)) ".
                    'for _, tagname in ipairs(KEYS) do '.
                    "redis.call('SADD', ARGV[7]..tagname, ARGV[9]) ".
                    'end '.
                    'end '.
                    "if (ARGV[15] == '1') then ".
                    "redis.call('SADD', ARGV[8], ARGV[9]) ".
                    'end '.
                    'if (oldTags ~= false) then '.
                    'return oldTags '.
                    'else '.
                    "return '' ".
                    'end',
            ],
            'removeByMatchingAnyTags' => [
                'code' => 'for i = 1, #KEYS, ARGV[6] do '.
                    'local prefixedTags = {} '.
                    'for x, tag in ipairs(KEYS) do '.
                    'prefixedTags[x] = ARGV[1]..tag '.
                    'end '.
                    "local keysToDel = redis.call('SUNION', unpack(prefixedTags, i, math.min(#prefixedTags, i + ARGV[6] - 1))) ".
                    'for _, keyname in ipairs(keysToDel) do '.
                    "redis.call('DEL', ARGV[2]..keyname) ".
                    "if (ARGV[5] == '1') then ".
                    "redis.call('SREM', ARGV[4], keyname) ".
                    'end '.
                    'end '.
                    "redis.call('DEL', unpack(prefixedTags, i, math.min(#prefixedTags, i + ARGV[6] - 1))) ".
                    "redis.call('SREM', ARGV[3], unpack(KEYS, i, math.min(#KEYS, i + ARGV[6] - 1))) ".
                    'end '.
                    'return true',
            ],
            'collectGarbage' => [
                'code' => 'local tagKeys = {} '.
                    'local expired = {} '.
                    'local expiredCount = 0 '.
                    'local notExpiredCount = 0 '.
                    'for _, tagName in ipairs(KEYS) do '.
                    "tagKeys = redis.call('SMEMBERS', ARGV[4]..tagName) ".
                    'for __, keyName in ipairs(tagKeys) do '.
                    "if (redis.call('EXISTS', ARGV[1]..keyName) == 0) then ".
                    'expiredCount = expiredCount + 1 '.
                    'expired[expiredCount] = keyName '.
                    /* Redis Lua scripts have a hard limit of 8000 parameters per command */
                    'if (expiredCount == 7990) then '.
                    "redis.call('SREM', ARGV[4]..tagName, unpack(expired)) ".
                    "if (ARGV[5] == '1') then ".
                    "redis.call('SREM', ARGV[3], unpack(expired)) ".
                    'end '.
                    'expiredCount = 0 '.
                    'expired = {} '.
                    'end '.
                    'else '.
                    'notExpiredCount = notExpiredCount + 1 '.
                    'end '.
                    'end '.
                    'if (expiredCount > 0) then '.
                    "redis.call('SREM', ARGV[4]..tagName, unpack(expired)) ".
                    "if (ARGV[5] == '1') then ".
                    "redis.call('SREM', ARGV[3], unpack(expired)) ".
                    'end '.
                    'end '.
                    'if (notExpiredCount == 0) then '.
                    "redis.call ('DEL', ARGV[4]..tagName) ".
                    "redis.call ('SREM', ARGV[2], tagName) ".
                    'end '.
                    'expired = {} '.
                    'expiredCount = 0 '.
                    'notExpiredCount = 0 '.
                    'end '.
                    'return true',
            ],
        ];
    }
}
