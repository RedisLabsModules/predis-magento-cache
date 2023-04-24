<?php

namespace Redis\Pmc\Feature;

use Redis\Pmc\Cache\Backend\Redis;

final class RedisTest extends FeatureTestCase
{
    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsReturnsCorrectIdArray(): void
    {
        $redis = $this->getClient();

        $redis->set(Redis::PREFIX_KEY.'1', 1);
        $redis->set(Redis::PREFIX_KEY.'2', 2);
        $redis->set(Redis::PREFIX_KEY.'3', 3);

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertSameValues(['1', '2', '3'], $backend->getIds());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsReturnsCorrectIdArrayWithNotMatchingTagsOption(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $redis->sadd(Redis::SET_IDS, [1, 2, 3]);

        $backend = new Redis($backendOptions);

        $this->assertSameValues(['1', '2', '3'], $backend->getIds());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetTagsReturnsCorrectTagsArray(): void
    {
        $redis = $this->getClient();

        $redis->sadd(Redis::SET_TAGS, ['tag1', 'tag2', 'tag3']);

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertSameValues(['tag1', 'tag2', 'tag3'], $backend->getTags());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsNotMatchingTagsReturnsIdsArrayNotMatchingGivenTags(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $redis->sadd(Redis::SET_IDS, [1, 2, 3, 4, 5]);
        $redis->sadd(Redis::PREFIX_TAG_IDS.'tag1', [1, 2]);
        $redis->sadd(Redis::PREFIX_TAG_IDS.'tag2', [4, 5]);

        $backend = new Redis($backendOptions);

        $this->assertSame(['3'], $backend->getIdsNotMatchingTags(['tag1', 'tag2']));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsNotMatchingTagsReturnsAllIdsIfNoTagsGiven(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $redis->sadd(Redis::SET_IDS, [1, 2, 3, 4, 5]);

        $backend = new Redis($backendOptions);

        $this->assertSameValues(['1', '2', '3', '4', '5'], $backend->getIdsNotMatchingTags());
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsNotMatchingTagsThrowsExceptionWithMissingNotMatchingOption(): void
    {
        $redis = $this->getClient();

        $redis->sadd(Redis::SET_IDS, [1, 2, 3, 4, 5]);

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->expectException(\Zend_Cache_Exception::class);
        $this->expectExceptionMessage('notMatchingTags option is currently disabled.');

        $backend->getIdsNotMatchingTags();
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetIdsMatchingAnyTagsReturnsIdsArrayMatchingGivenTags(): void
    {
        $redis = $this->getClient();

        $redis->sadd(Redis::PREFIX_TAG_IDS.'tag1', [1, 2]);
        $redis->sadd(Redis::PREFIX_TAG_IDS.'tag2', [4, 5]);

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertSameValues(['1', '2', '4', '5'], $backend->getIdsMatchingAnyTags(['tag1', 'tag2']));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetFillingPercentage(): void
    {
        $backend = new Redis($this->getDefaultBackendOptions());
        $actualResponse = $backend->getFillingPercentage();

        $this->assertGreaterThanOrEqual(0, $actualResponse);
        $this->assertLessThanOrEqual(100, $actualResponse);
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testGetMetadatas(): void
    {
        $redis = $this->getClient();
        $expectedResponse = [
            'expire' => false,
            'tags' => ['tag1', 'tag2', 'tag3'],
            'mtime' => '10000',
        ];

        $this->assertSame(3, $redis->hset(
            Redis::PREFIX_KEY.'1',
            Redis::FIELD_TAGS,
            'tag1,tag2,tag3',
            Redis::FIELD_MTIME,
            10000,
            Redis::FIELD_INF,
            1
        ));

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertSame($expectedResponse, $backend->getMetadatas('1'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testTouchGivesExtraLifetimeToGivenCacheId(): void
    {
        $redis = $this->getClient();

        $this->assertSame(1, $redis->hset(
            Redis::PREFIX_KEY.'1',
            Redis::FIELD_INF,
            '0'
        ));

        $this->assertSame(1, $redis->expire(Redis::PREFIX_KEY.'1', 10));

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertTrue($backend->touch('1', 10));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsCorrectDataForGivenId(): void
    {
        $redis = $this->getClient();

        $redis->hset(Redis::PREFIX_KEY.'key', Redis::FIELD_DATA, 'data');

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertSame('data', $backend->load('key'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testLoadReturnsCorrectDecodedDataForGivenId(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['compression_lib' => 'gzip']);
        $compressionPrefix = substr('gzip', 0, 2).Redis::COMPRESS_PREFIX;

        $redis->hset(
            Redis::PREFIX_KEY.'key',
            Redis::FIELD_DATA,
            $compressionPrefix.gzcompress('word1,word2,word3', 1)
        );

        $backend = new Redis($backendOptions);

        $this->assertSame('word1,word2,word3', $backend->load('key'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testTestReturnsIdExpirationTime(): void
    {
        $redis = $this->getClient();

        $redis->hset(Redis::PREFIX_KEY.'1', Redis::FIELD_MTIME, 10000);

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertSame('10000', $backend->test('1'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testSaveCorrectlySaveDataAndTags(): void
    {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertTrue($backend->save('hello,world', 'id', ['tag1', 'tag2']));
        $this->assertSame('hello,world', $backend->load('id'));
        $this->assertSameValues(['tag1', 'tag2'], $redis->smembers(Redis::SET_TAGS));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testSaveRemovesOldTagsFromAlreadyExistingId(): void
    {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertTrue($backend->save('hello,world', 'id', ['tag1', 'tag2']));
        $this->assertSame('hello,world', $backend->load('id'));
        $this->assertSameValues(['tag1', 'tag2'], $redis->smembers(Redis::SET_TAGS));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));

        $this->assertTrue($backend->save('hello,world', 'id', ['tag3', 'tag4']));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag3', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag4', 'id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testSaveAddIdToGlobalIdListOnNotMatchingTagsOption(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $backend = new Redis($backendOptions);

        $this->assertTrue($backend->save('hello,world', 'id', ['tag1', 'tag2']));
        $this->assertSame('hello,world', $backend->load('id'));
        $this->assertSameValues(['tag1', 'tag2'], $redis->smembers(Redis::SET_TAGS));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::SET_IDS, 'id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testSaveCorrectlySaveDataAndTagsWithUseLuaOption(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['use_lua' => true]);

        $backend = new Redis($backendOptions);

        $this->assertTrue($backend->save('hello,world', 'id', ['tag1', 'tag2']));
        $this->assertSame('hello,world', $backend->load('id'));
        $this->assertSameValues(['tag1', 'tag2'], $redis->smembers(Redis::SET_TAGS));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testRemoveCorrectlyRemovesIdAndAllAssociatedTags(): void
    {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());

        $this->assertTrue($backend->save('hello,world', 'id', ['tag1', 'tag2']));
        $this->assertTrue($backend->remove('id'));
        $this->assertSame(0, $redis->exists(Redis::PREFIX_KEY.'id'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testRemoveCorrectlyRemovesIdFromGlobalIdList(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $backend = new Redis($backendOptions);

        $this->assertTrue($backend->save('hello,world', 'id', ['tag1', 'tag2']));
        $this->assertTrue($backend->remove('id'));
        $this->assertSame(0, $redis->exists(Redis::PREFIX_KEY.'id'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id'));
        $this->assertSame(0, $redis->sismember(Redis::SET_IDS, 'id'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanFlushesDBInCleaningAllMode(): void
    {
        $redis = $this->getClient();

        $redis->set('key', 'value');
        $redis->sadd('skey', ['member1', 'member2']);

        $backend = new Redis($this->getDefaultBackendOptions());
        $backend->clean();

        $this->assertEmpty($redis->keys('*'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanRemovesExpiredIdsAndAllAssociatedTagsInCleaningOldMode(): void
    {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());
        $this->assertTrue($backend->save('hello,world', 'id:1', ['tag1', 'tag2'], 1));
        $this->assertTrue($backend->save('hello,world', 'id:2', ['tag1', 'tag2']));

        sleep(2);

        $backend->clean(\Zend_Cache::CLEANING_MODE_OLD);

        $this->assertSame(0, $redis->exists(Redis::PREFIX_KEY.'id:1'));
        $this->assertSame(1, $redis->exists(Redis::PREFIX_KEY.'id:2'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id:1'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id:1'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id:2'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id:2'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanRemovesFromGlobalIdListInCleaningOldModeWithNonMatchingTagsOption(): void
    {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $backend = new Redis($backendOptions);
        $this->assertTrue($backend->save('hello,world', 'id:1', ['tag1', 'tag2'], 1));
        $this->assertTrue($backend->save('hello,world', 'id:2', ['tag1', 'tag2']));

        sleep(2);

        $backend->clean(\Zend_Cache::CLEANING_MODE_OLD);

        $this->assertSame(0, $redis->exists(Redis::PREFIX_KEY.'id:1'));
        $this->assertSame(1, $redis->exists(Redis::PREFIX_KEY.'id:2'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id:1'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id:1'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id:2'));
        $this->assertSame(1, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id:2'));
        $this->assertSame(0, $redis->sismember(Redis::SET_IDS, 'id:1'));
        $this->assertSame(1, $redis->sismember(Redis::SET_IDS, 'id:2'));
    }

    /**
     * @throws \Zend_Cache_Exception
     */
    public function testCleanRemovesEmptyTagsInCleaningOldMode(): void
    {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());
        $this->assertTrue($backend->save('hello,world', 'id:1', ['tag1', 'tag2'], 1));

        sleep(2);

        $backend->clean(\Zend_Cache::CLEANING_MODE_OLD);

        $this->assertSame(0, $redis->exists(Redis::PREFIX_KEY.'id:1'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag1', 'id:1'));
        $this->assertSame(0, $redis->sismember(Redis::PREFIX_TAG_IDS.'tag2', 'id:1'));
        $this->assertSame(0, $redis->exists(Redis::PREFIX_TAG_IDS.'tag1'));
        $this->assertSame(0, $redis->exists(Redis::PREFIX_TAG_IDS.'tag2'));
        $this->assertSame(0, $redis->sismember(Redis::SET_TAGS, 'tag1'));
        $this->assertSame(0, $redis->sismember(Redis::SET_TAGS, 'tag2'));
    }

    /**
     * @dataProvider matchingTagsProvider
     * @throws \Zend_Cache_Exception
     */
    public function testCleanRemovesIdsByMatchingTagsInCleaningModeMatchingTag(
        array $firstIdTags,
        array $secondIdTags,
        array $matchingTags,
        int $firstIdStatus,
        int $secondIdStatus
    ): void {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());
        $this->assertTrue($backend->save('hello,world', 'id:1', $firstIdTags));
        $this->assertTrue($backend->save('hello,world', 'id:2', $secondIdTags));

        $backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_TAG, $matchingTags);

        $this->assertSame($firstIdStatus, $redis->exists(Redis::PREFIX_KEY.'id:1'));
        $this->assertSame($secondIdStatus, $redis->exists(Redis::PREFIX_KEY.'id:2'));
    }

    /**
     * @dataProvider nonMatchingTagsProvider
     * @throws \Zend_Cache_Exception
     */
    public function testCleanRemovesIdsNotMatchingTagsInCleaningModeNotMatchingTag(
        array $firstIdTags,
        array $secondIdTags,
        array $nonMatchingTags,
        int $firstIdStatus,
        int $secondIdStatus
    ): void {
        $redis = $this->getClient();
        $backendOptions = array_merge($this->getDefaultBackendOptions(), ['notMatchingTags' => true]);

        $backend = new Redis($backendOptions);
        $this->assertTrue($backend->save('hello,world', 'id:1', $firstIdTags));
        $this->assertTrue($backend->save('hello,world', 'id:2', $secondIdTags));

        $backend->clean(\Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG, $nonMatchingTags);

        $this->assertSame($firstIdStatus, $redis->exists(Redis::PREFIX_KEY.'id:1'));
        $this->assertSame($secondIdStatus, $redis->exists(Redis::PREFIX_KEY.'id:2'));
    }

    /**
     * @dataProvider matchingAnyTagsProvider
     * @throws \Zend_Cache_Exception
     */
    public function testCleanRemovesIdsByMatchingAnyTagsInCleaningModeMatchingAnyTag(
        array $firstIdTags,
        array $secondIdTags,
        array $matchingTags,
        int $firstIdStatus,
        int $secondIdStatus
    ): void {
        $redis = $this->getClient();

        $backend = new Redis($this->getDefaultBackendOptions());
        $this->assertTrue($backend->save('hello,world', 'id:1', $firstIdTags));
        $this->assertTrue($backend->save('hello,world', 'id:2', $secondIdTags));

        $backend->clean(\Zend_Cache::CLEANING_MODE_MATCHING_TAG, $matchingTags);

        $this->assertSame($firstIdStatus, $redis->exists(Redis::PREFIX_KEY.'id:1'));
        $this->assertSame($secondIdStatus, $redis->exists(Redis::PREFIX_KEY.'id:2'));
    }

    public function matchingTagsProvider(): array
    {
        return [
            'with both non matching tag' => [
                ['tag1', 'tag2'],
                ['tag2', 'tag3'],
                ['tag4'],
                1,
                1,
            ],
            'with both matching tag' => [
                ['tag1', 'tag2'],
                ['tag2', 'tag3'],
                ['tag2'],
                0,
                0,
            ],
            'with both matching two tags' => [
                ['tag1', 'tag2', 'tag3'],
                ['tag1', 'tag2', 'tag4'],
                ['tag2', 'tag1'],
                0,
                0,
            ],
            'with one mismatching tag' => [
                ['tag1', 'tag2', 'tag3'],
                ['tag1', 'tag2', 'tag4'],
                ['tag1', 'tag3'],
                0,
                1,
            ],
        ];
    }

    public function nonMatchingTagsProvider(): array
    {
        return [
            'with both non matching tag' => [
                ['tag1', 'tag2'],
                ['tag2', 'tag3'],
                ['tag4'],
                0,
                0,
            ],
            'with both matching tag' => [
                ['tag1', 'tag2'],
                ['tag2', 'tag3'],
                ['tag2'],
                1,
                1,
            ],
            'with both matching two tags' => [
                ['tag1', 'tag2', 'tag3'],
                ['tag1', 'tag2', 'tag4'],
                ['tag2', 'tag1'],
                1,
                1,
            ],
            'with one mismatching tag' => [
                ['tag1', 'tag2', 'tag3'],
                ['tag1', 'tag2', 'tag4'],
                ['tag1', 'tag3'],
                1,
                1,
            ],
        ];
    }

    public function matchingAnyTagsProvider(): array
    {
        return [
            'with both non matching tag' => [
                ['tag1', 'tag2'],
                ['tag2', 'tag3'],
                ['tag4'],
                1,
                1,
            ],
            'with both matching tag' => [
                ['tag1', 'tag2'],
                ['tag2', 'tag3'],
                ['tag2'],
                0,
                0,
            ],
            'with both matching two tags' => [
                ['tag1', 'tag2', 'tag3'],
                ['tag1', 'tag2', 'tag4'],
                ['tag2', 'tag1'],
                0,
                0,
            ],
            'with one mismatching tag' => [
                ['tag1', 'tag2', 'tag3'],
                ['tag1', 'tag2', 'tag4'],
                ['tag1', 'tag3'],
                0,
                1,
            ],
        ];
    }

    private function getDefaultBackendOptions(): array
    {
        return [
            'server' => constant('REDIS_SERVER_HOST'),
            'port' => constant('REDIS_SERVER_PORT'),
            'database' => constant('REDIS_SERVER_DBNUM'),
        ];
    }
}
