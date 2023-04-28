<?php

namespace Redis\Pmc\Cache\Backend;

use Predis\ClientInterface;
use Predis\Pipeline\Pipeline;

class Redis extends \Zend_Cache_Backend implements \Zend_Cache_Backend_ExtendedInterface
{
    public const SET_IDS = 'zc:ids';
    public const SET_TAGS = 'zc:tags';

    public const PREFIX_KEY = 'zc:k:';
    public const PREFIX_TAG_IDS = 'zc:ti:';

    public const FIELD_DATA = 'd';
    public const FIELD_MTIME = 'm';
    public const FIELD_TAGS = 't';
    public const FIELD_INF = 'i';
    public const REDIS_LIB_NAME = 'cache_backend';

    public const MAX_LIFETIME = 2592000; /* Redis backend limit */
    public const COMPRESS_PREFIX = ":\x1f\x8b";

    protected ClientInterface $_client;
    protected bool $_notMatchingTags = false;
    protected int $_lifetimeLimit = self::MAX_LIFETIME; /* Redis backend limit */
    protected int|bool $_compressTags = 1;
    protected int|bool $_compressData = 1;
    protected int $_compressThreshold = 20480;
    protected string $_compressionLib;
    protected int $_automaticCleaningFactor = 0;
    protected \DateTime $_dateTime;

    /**
     * Defines possible available by default PHP compression libraries.
     */
    protected array $_availableCompressionLibraries = [
        'snappy',
        'lz4',
        'zstd',
        'lzf',
    ];

    /**
     * Lua scripts to perform CRUD operations.
     */
    protected array $_luaScripts = [
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

    /**
     * On large data sets SUNION slows down considerably when used with too many arguments
     * so this is used to chunk the SUNION into a few commands where the number of set ids
     * exceeds this setting.
     */
    protected int $_sUnionChunkSize = 500;

    /**
     * Maximum number of ids to be removed at a time.
     */
    protected int $_removeChunkSize = 10000;
    protected bool $_useLua = false;
    protected int $_autoExpireLifetime = 0;
    protected bool $_autoExpireRefreshOnLoad = false;

    /**
     * Lua's unpack() has a limit on the size of the table imposed by
     * the number of Lua stack slots that a C function can use.
     * This value is defined by LUAI_MAXCSTACK in luaconf.h and for Redis it is set to 8000.
     *
     * @see https://github.com/antirez/redis/blob/b903145/deps/lua/src/luaconf.h#L439
     */
    protected int $_luaMaxCStack = 5000;

    /**
     * @throws \Zend_Cache_Exception
     */
    public function __construct(
        array $options = [],
        ?FactoryInterface $factory = null,
        ?\DateTimeInterface $dateTime = null
    ) {
        $factory = $factory ?? new ClientFactory();
        $this->_dateTime = $dateTime ?? new \DateTime();
        $this->_client = $factory->create($options);

        if (isset($options['automatic_cleaning_factor'])) {
            $this->_automaticCleaningFactor = (int) $options['automatic_cleaning_factor'];
        }

        if (isset($options['notMatchingTags'])) {
            $this->_notMatchingTags = (bool) $options['notMatchingTags'];
        }

        if (isset($options['lifetimelimit'])) {
            $this->_lifetimeLimit = (int) min($options['lifetimelimit'], self::MAX_LIFETIME);
        }

        if (isset($options['use_lua'])) {
            $this->_useLua = (bool) $options['use_lua'];
        }

        if (isset($options['auto_expire_lifetime'])) {
            $this->_autoExpireLifetime = (int) $options['auto_expire_lifetime'];
        }

        if (isset($options['auto_expire_refresh_on_load'])) {
            $this->_autoExpireRefreshOnLoad = (bool) $options['auto_expire_refresh_on_load'];
        }

        $this->_setCompressionConfiguration($options);
    }

    /**
     * {@inheritDoc}
     */
    public function getIds()
    {
        if ($this->_notMatchingTags) {
            return $this->_client->smembers(self::SET_IDS);
        }

        $keys = $this->_client->keys(self::PREFIX_KEY.'*');
        $prefixLength = strlen(self::PREFIX_KEY);

        return array_map(static function ($key) use ($prefixLength) {
            return substr($key, $prefixLength);
        }, $keys);
    }

    /**
     * {@inheritDoc}
     */
    public function getTags()
    {
        return $this->_client->smembers(self::SET_TAGS);
    }

    /**
     * {@inheritDoc}
     */
    public function getIdsMatchingTags($tags = [])
    {
        if (empty($tags)) {
            return $tags;
        }

        return $this->_client->sinter($this->_processTagIds($tags));
    }

    /**
     * {@inheritDoc}
     */
    public function getIdsNotMatchingTags($tags = [])
    {
        if (!$this->_notMatchingTags) {
            \Zend_Cache::throwException('notMatchingTags option is currently disabled.');
        }

        if (empty($tags)) {
            return $this->_client->smembers(self::SET_IDS);
        }

        return $this->_client->sdiff([self::SET_IDS, ...$this->_processTagIds($tags)]);
    }

    /**
     * {@inheritDoc}
     */
    public function getIdsMatchingAnyTags($tags = [])
    {
        if (empty($tags)) {
            return $tags;
        }

        $ids = [];
        $chunks = array_chunk($tags, $this->_sUnionChunkSize);

        foreach ($chunks as $chunk) {
            $ids = array_merge($ids, $this->_client->sunion($this->_processTagIds($chunk)));

            if (count($chunks) > 1) {
                $ids = array_unique($ids);    // since we are chunking requests, we must de-duplicate member names
            }
        }

        return $ids;
    }

    /**
     * {@inheritDoc}
     */
    public function getFillingPercentage()
    {
        $maxMemory = $this->_client->config('GET', 'maxmemory');

        if (0 === (int) $maxMemory['maxmemory']) {
            return 1;
        }

        $info = $this->_client->info('Memory');

        return (int) round(
            (int) $info['Memory']['used_memory'] / $maxMemory['maxmemory'] * 100
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getMetadatas($id)
    {
        [$tags, $mtime, $inf] = $this->_client->hmget(
            self::PREFIX_KEY.$id,
            self::FIELD_TAGS, self::FIELD_MTIME,
            self::FIELD_INF
        );

        if (!$mtime) {
            return false;
        }

        $tags = explode(',', $this->_decodeData($tags));
        $expire = '1' === $inf ? false : $this->_dateTime->getTimestamp() + $this->_client->ttl(self::PREFIX_KEY.$id);

        return [
            'expire' => $expire,
            'tags' => $tags,
            'mtime' => $mtime,
        ];
    }

    /**
     * {@inheritDoc}
     */
    public function touch($id, $extraLifetime)
    {
        $inf = $this->_client->hget(self::PREFIX_KEY.$id, self::FIELD_INF);

        if ('0' == $inf) {
            $ttl = $this->_client->ttl(self::PREFIX_KEY.$id);
            $expire = $ttl + $extraLifetime;

            return (bool) $this->_client->expire(self::PREFIX_KEY.$id, $expire);
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function getCapabilities()
    {
        return [
            'automatic_cleaning' => ($this->_automaticCleaningFactor > 0),
            'tags' => true,
            'expired_read' => false,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true,
        ];
    }

    /**
     * {@inheritDoc}
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $data = $this->_client->hget(self::PREFIX_KEY.$id, self::FIELD_DATA);

        if (empty($data)) {
            return false;
        }

        $decodedData = $this->_decodeData($data);

        if (0 === $this->_autoExpireLifetime || !$this->_autoExpireRefreshOnLoad) {
            return $decodedData;
        }

        if (!$this->_matchesAutoExpiringPattern($id)) {
            return $decodedData;
        }

        $this->_client->expire(self::PREFIX_KEY.$id, min($this->_autoExpireLifetime, self::MAX_LIFETIME));

        return $decodedData;
    }

    protected function _encodeData(string $data, int $level): string
    {
        if ($this->_compressionLib && 0 !== $level && strlen($data) >= $this->_compressThreshold) {
            $data = match ($this->_compressionLib) {
                'snappy' => snappy_compress($data),
                'lzf' => lzf_compress($data),
                'l4z' => lz4_compress($data, $level),
                'zstd' => zstd_compress($data, $level),
                'gzip' => gzcompress($data, $level),
                default => throw new \InvalidArgumentException("Unrecognized 'compression_lib'."),
            };

            if (!$data) {
                throw new \RuntimeException('Could not compress cache data.');
            }

            return $this->_compressPrefix.$data;
        }

        return $data;
    }

    /**
     * @param bool|string $data
     *
     * @return string
     */
    protected function _decodeData($data)
    {
        $actualData = substr($data, 5);

        try {
            if (self::COMPRESS_PREFIX === substr($data, 2, 3)) {
                switch (substr($data, 0, 2)) {
                    case 'sn': return snappy_uncompress($actualData);
                    case 'lz': return lzf_decompress($actualData);
                    case 'l4': return lz4_uncompress($actualData);
                    case 'zs': return zstd_uncompress($actualData);
                    case 'gz': case 'zc': return gzuncompress($actualData);
                }
            }
        } catch (\Exception $e) {
            $data = false;
        }

        return $data;
    }

    /**
     * {@inheritDoc}
     */
    public function test($id)
    {
        $mtime = $this->_client->hGet(self::PREFIX_KEY.$id, self::FIELD_MTIME);

        return $mtime ?: false;
    }

    /**
     * Get the lifetime.
     *
     * if $specificLifetime is not false, the given specific lifetime is used
     * else, the global lifetime is used
     *
     * @param  int $specificLifetime
     *
     * @return int Cache lifetime
     */
    public function getLifetime($specificLifetime)
    {
        // Lifetimes set via Layout XMLs get parsed as string so bool(false) becomes string("false")
        if ('false' === $specificLifetime) {
            $specificLifetime = false;
        }

        return parent::getLifetime($specificLifetime);
    }

    /**
     * Get the auto expiring lifetime.
     *
     * Mainly a workaround for the issues that arise due to the fact that
     * Magento's Enterprise_PageCache module doesn't set any expiry.
     *
     * @return int|null Cache lifetime
     */
    protected function _getAutoExpiringLifetime($lifetime, string $id): ?int
    {
        if ($lifetime || !$this->_autoExpireLifetime) {
            return $lifetime;
        }

        $matches = $this->_matchesAutoExpiringPattern($id);

        if (!$matches) {
            return $lifetime;
        }

        if ($this->_autoExpireLifetime > 0) {
            return $this->_autoExpireLifetime;
        }

        return $lifetime;
    }

    protected function _matchesAutoExpiringPattern($id)
    {
        return str_contains($id, 'REQEST');
    }

    /**
     * {@inheritDoc}
     *
     * @throws \Zend_Cache_Exception
     */
    public function save($data, $id, $tags = [], $specificLifetime = false)
    {
        $lifetime = $this->_getAutoExpiringLifetime($this->getLifetime($specificLifetime), $id);
        $oldTags = $this->_client->hget(self::PREFIX_KEY.$id, self::FIELD_TAGS);
        $oldTags = (null !== $oldTags) ? explode(',', $oldTags) : [];

        if ($this->_useLua) {
            $result = $this->_executeLuaScript('save', $tags, [
                self::PREFIX_KEY,
                self::FIELD_DATA,
                self::FIELD_TAGS,
                self::FIELD_MTIME,
                self::FIELD_INF,
                self::SET_TAGS,
                self::PREFIX_TAG_IDS,
                self::SET_IDS,
                $id,
                $this->_encodeData($data, $this->_compressData),
                $this->_encodeData(implode(',', $tags), $this->_compressTags),
                $this->_dateTime->getTimestamp(),
                $lifetime ? 0 : 1,
                min($lifetime, self::MAX_LIFETIME),
                $this->_notMatchingTags ? 1 : 0,
            ]);

            // Process removed tags if cache entry already existed
            if ($result) {
                $oldTags = explode(',', $this->_decodeData($result));
                if ($remTags = ($oldTags ? array_diff($oldTags, $tags) : false)) {
                    $this->_client->pipeline(function (Pipeline $pipeline) use ($remTags, $id) {
                        // Update the id list for each tag
                        foreach ($remTags as $tag) {
                            $pipeline->srem(self::PREFIX_TAG_IDS.$tag, $id);
                        }
                    });
                }
            }

            return true;
        }

        $this->_client->pipeline(function (Pipeline $pipeline) use ($data, $id, $tags, $oldTags, $lifetime) {
            $pipeline->multi();

            $pipeline->hset(
                self::PREFIX_KEY.$id,
                self::FIELD_DATA,
                $this->_encodeData($data, $this->_compressData),
                self::FIELD_TAGS,
                $this->_encodeData(implode(',', $tags), $this->_compressTags),
                self::FIELD_MTIME,
                $this->_dateTime->getTimestamp(),
                self::FIELD_INF,
                is_null($lifetime) ? 1 : 0
            );

            if (false !== $lifetime && !is_null($lifetime)) {
                $pipeline->expire(self::PREFIX_KEY.$id, min($lifetime, self::MAX_LIFETIME));
            }

            if ($tags) {
                // Update the list with all the tags
                $pipeline->sadd(self::SET_TAGS, $tags);

                // Update the id list for each tag
                foreach ($tags as $tag) {
                    $pipeline->sadd(self::PREFIX_TAG_IDS.$tag, $id);
                }
            }

            if ($remTags = ($oldTags ? array_diff($oldTags, $tags) : false)) {
                // Update the id list for each tag
                foreach ($remTags as $tag) {
                    $pipeline->srem(self::PREFIX_TAG_IDS.$tag, $id);
                }
            }

            // Update the list with all the ids
            if ($this->_notMatchingTags) {
                $pipeline->sadd(self::SET_IDS, $id);
            }

            $pipeline->exec();
        });

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function remove($id)
    {
        $tags = $this->_client->hget(self::PREFIX_KEY.$id, self::FIELD_TAGS);
        $tags = (null !== $tags) ? explode(',', $tags) : [];

        $result = $this->_client->pipeline(function (Pipeline $pipeline) use ($id, $tags) {
            $pipeline->multi();

            $pipeline->del(self::PREFIX_KEY.$id);

            if ($this->_notMatchingTags) {
                $pipeline->srem(self::SET_IDS, $id);
            }

            foreach ($tags as $tag) {
                $pipeline->srem(self::PREFIX_TAG_IDS.$tag, $id);
            }

            $pipeline->exec();
        });

        return (bool) $result[0];
    }

    /**
     * {@inheritDoc}
     *
     * @throws \Zend_Cache_Exception
     */
    public function clean($mode = \Zend_Cache::CLEANING_MODE_ALL, $tags = [])
    {
        try {
            if (empty($tags)) {
                switch ($mode) {
                    case \Zend_Cache::CLEANING_MODE_ALL:
                        $this->_client->flushdb();

                        return true;
                    case \Zend_Cache::CLEANING_MODE_OLD:
                        $this->_collectGarbage();

                        return true;
                    default: return true;
                }
            }

            switch ($mode) {
                case \Zend_Cache::CLEANING_MODE_MATCHING_TAG:
                    $this->_removeByMatchingTags($tags);

                    return true;
                case \Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
                    $this->_removeByNotMatchingTags($tags);

                    return true;
                case \Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
                    $this->_removeByMatchingAnyTags($tags);

                    return true;
                default:
                    \Zend_Cache::throwException('Invalid mode for clean() method: '.$mode);
            }
        } catch (\Throwable $e) {
            \Zend_Cache::throwException('Error cleaning cache on mode '.$mode.': '.$e->getMessage(), $e);
        }
    }

    /**
     * Removes all ids that matches given tags.
     */
    protected function _removeByMatchingTags(array $tags): void
    {
        $ids = $this->getIdsMatchingTags($tags);

        if (!empty($ids)) {
            $this->_removeIds($ids);
        }
    }

    /**
     * Removes all ids that not matches given tags.
     */
    protected function _removeByNotMatchingTags(array $tags): void
    {
        $ids = $this->getIdsNotMatchingTags($tags);

        if (!empty($ids)) {
            $this->_removeIds($ids);
        }
    }

    /**
     * Removes all ids that matches any of given tags.
     *
     * @throws \Zend_Cache_Exception
     */
    protected function _removeByMatchingAnyTags(array $tags): void
    {
        if ($this->_useLua) {
            $tags = array_chunk($tags, $this->_sUnionChunkSize);

            foreach ($tags as $chunk) {
                $this->_executeLuaScript('removeByMatchingAnyTags', $chunk, [
                    self::PREFIX_TAG_IDS,
                    self::PREFIX_KEY,
                    self::SET_TAGS,
                    self::SET_IDS,
                    $this->_notMatchingTags ? 1 : 0,
                    $this->_luaMaxCStack,
                ]);
            }

            return;
        }

        $ids = $this->getIdsMatchingAnyTags($tags);

        if (!empty($ids)) {
            $this->_removeIds($ids);
        }

        $this->_client->pipeline(function (Pipeline $pipeline) use ($tags) {
            $pipeline->multi();

            $pipeline->del($this->_processTagIds($tags));
            $pipeline->srem(self::SET_TAGS, ...$tags);

            $pipeline->exec();
        });
    }

    /**
     * Remove id keys.
     */
    private function _removeIds(array $ids): void
    {
        $ids = array_chunk($ids, $this->_removeChunkSize);

        $this->_client->pipeline(function (Pipeline $pipeline) use ($ids) {
            foreach ($ids as $chunk) {
                $pipeline->multi();

                $pipeline->del($this->_processIds($chunk));

                if ($this->_notMatchingTags) {
                    $pipeline->srem(self::SET_IDS, ...$chunk);
                }

                $pipeline->exec();
            }
        });
    }

    /**
     * Apply prefix for given tags ids.
     */
    protected function _processTagIds(array $tags): array
    {
        array_walk($tags, [$this, '_applyPrefix'], self::PREFIX_TAG_IDS);

        return $tags;
    }

    /**
     * Apply prefix for given ids.
     */
    protected function _processIds(array $ids): array
    {
        array_walk($ids, [$this, '_applyPrefix'], self::PREFIX_KEY);

        return $ids;
    }

    /**
     * Callback that applies given prefix.
     */
    protected function _applyPrefix(&$item, $index, $prefix)
    {
        $item = $prefix.$item;
    }

    /**
     * Clean up expired tags and id's.
     *
     * @throws \Zend_Cache_Exception
     */
    protected function _collectGarbage(): void
    {
        if ($this->_useLua) {
            $allTags = array_chunk($this->getTags(), 10);

            foreach ($allTags as $chunk) {
                $this->_executeLuaScript('collectGarbage', $chunk, [
                    self::PREFIX_KEY,
                    self::SET_TAGS,
                    self::SET_IDS,
                    self::PREFIX_TAG_IDS,
                    $this->_notMatchingTags ? 1 : 0,
                ]);

                usleep(20000);
            }

            return;
        }

        $tags = $this->_client->smembers(self::SET_TAGS);

        foreach ($tags as $tag) {
            $tagMembers = $this->_client->smembers(self::PREFIX_TAG_IDS.$tag);
            $expiredMembersCount = 0;
            $existingStatuses = [];

            if (!empty($tagMembers)) {
                foreach ($tagMembers as $member) {
                    $existingStatuses[$member] = $this->_client->exists(self::PREFIX_KEY.$member);
                }

                $expiredMembers = array_filter($existingStatuses, static function ($status) {
                    return $status < 1;
                });

                $expiredMembersCount = count($expiredMembers);

                $this->_client->pipeline(function (Pipeline $pipeline) use ($expiredMembers, $tag) {
                    foreach ($expiredMembers as $member => $status) {
                        $pipeline->srem(self::PREFIX_TAG_IDS.$tag, $member);

                        if ($this->_notMatchingTags) {
                            $pipeline->srem(self::SET_IDS, $member);
                        }
                    }
                });
            }

            if ($expiredMembersCount === count($tagMembers)) {
                $this->_client->del(self::PREFIX_TAG_IDS.$tag);
                $this->_client->srem(self::SET_TAGS, $tag);
            }
        }
    }

    /**
     * Set compression configuration from given options.
     */
    protected function _setCompressionConfiguration(array $options): void
    {
        if (isset($options['compress_tags'])) {
            $this->_compressTags = (int) $options['compress_tags'];
        }

        if (isset($options['compress_data'])) {
            $this->_compressData = (int) $options['compress_data'];
        }

        if (isset($options['compression_lib'])) {
            $this->_compressionLib = (string) $options['compression_lib'];
        }

        if (isset($options['compress_threshold'])) {
            if ((int) $options['compress_threshold'] < 1) {
                $this->_compressThreshold = 1;
            } else {
                $this->_compressThreshold = (int) $options['compress_threshold'];
            }
        }

        $existingLibName = '';

        foreach ($this->_availableCompressionLibraries as $library) {
            if (function_exists($library.'_compress')) {
                $existingLibName = $library;
                break;
            }
        }

        if ('' === $existingLibName) {
            $existingLibName = 'gzip';
        }

        switch ($existingLibName) {
            case 'lz4':
                if ($this->_isSatisfiedCompressionLibVersion($existingLibName, '0.3.0')) {
                    $this->_compressTags = $this->_compressTags > 1;
                    $this->_compressData = $this->_compressData > 1;
                }

                break;
            case 'zstd':
                if ($this->_isSatisfiedCompressionLibVersion($existingLibName, '0.4.13')) {
                    $this->_compressTags = $this->_compressTags > 1;
                    $this->_compressData = $this->_compressData > 1;
                }

                break;
        }

        $this->_compressionLib = $existingLibName;
        $this->_compressPrefix = substr($this->_compressionLib, 0, 2).self::COMPRESS_PREFIX;
    }

    /**
     * Checks whether actual version of given compression library is lower than required.
     */
    private function _isSatisfiedCompressionLibVersion(string $compressionLib, string $version): bool
    {
        $actualVersion = phpversion($compressionLib);

        return version_compare($version, $actualVersion) < 0;
    }

    /**
     * Executes given LUA script with provided arguments.
     *
     * @throws \Zend_Cache_Exception
     */
    protected function _executeLuaScript(string $scriptName, array $keys = [], array $arguments = []): mixed
    {
        if (!array_key_exists($scriptName, $this->_luaScripts)) {
            \Zend_Cache::throwException('Non existing script with following name.');
        }

        $redisVersion = $this->_getRedisServerVersion();

        if (version_compare($redisVersion, '7.0.0') >= 0) {
            $this->_loadRedisFunction($scriptName);

            return $this->_client->fcall($scriptName, $keys, ...$arguments);
        }

        return $this->_client->eval($this->_luaScripts[$scriptName]['code'], count($keys), ...$keys, ...$arguments);
    }

    private function _getRedisServerVersion(): string
    {
        $info = $this->_client->info();

        return $info['Server']['redis_version'];
    }

    /**
     * Loads given LUA script as redis function.
     *
     * @throws \Zend_Cache_Exception
     */
    private function _loadRedisFunction(string $scriptName): void
    {
        $libName = self::REDIS_LIB_NAME;

        // Check if library with given name exists
        $functions = $this->_client->executeRaw(['FUNCTION', 'LIST', 'LIBRARYNAME', $libName]);

        // Check if given function exists within library above
        if (!empty($functions)) {
            $libInfo = $functions[0][5][0];

            if ($libInfo[1] === $scriptName) {
                return;
            }
        }

        // Process LUA script to match Redis functions API
        $function = "#!lua name={$libName} \n redis.register_function('{$scriptName}', function(keys, args) ";
        $processedFunctionCode = str_replace(['ARGV', 'KEYS'], ['args', 'keys'], $this->_luaScripts[$scriptName]['code']);
        $function .= $processedFunctionCode.' end)';

        $this->_client->function->load($function);
    }
}
