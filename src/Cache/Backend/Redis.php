<?php

namespace Redis\Pmc\Cache\Backend;

use Exception;
use Predis\Client;
use Predis\Pipeline\Pipeline;
use RuntimeException;
use Throwable;
use Zend_Cache;
use Zend_Cache_Backend;
use Zend_Cache_Backend_ExtendedInterface;
use Zend_Cache_Exception;

class Redis extends Zend_Cache_Backend implements Zend_Cache_Backend_ExtendedInterface
{
    private const SET_IDS         = 'zc:ids';
    private const SET_TAGS        = 'zc:tags';

    private const PREFIX_KEY      = 'zc:k:';
    private const PREFIX_TAG_IDS  = 'zc:ti:';

    private const FIELD_DATA      = 'd';
    private const FIELD_MTIME     = 'm';
    private const FIELD_TAGS      = 't';
    private const FIELD_INF       = 'i';
    private const REDIS_LIB_NAME  = 'cache_backend';

    private const MAX_LIFETIME    = 2592000; /* Redis backend limit */
    private const COMPRESS_PREFIX = ":\x1f\x8b";

    protected Client $_client;
    protected bool $_notMatchingTags = false;
    protected int $_lifetimeLimit = self::MAX_LIFETIME; /* Redis backend limit */
    protected int|bool $_compressTags = 1;
    protected int|bool $_compressData = 1;
    protected int $_compressThreshold = 20480;
    protected string $_compressionLib;
    protected string $_compressPrefix;
    protected int $_automaticCleaningFactor = 0;

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
     *
     * @var array
     */
    protected array $_luaScripts = [
        'save' => [
            "sha1" => '1617c9fb2bda7d790bb1aaa320c1099d81825e64',
            "code" => "local oldTags = redis.call('HGET', ARGV[1]..ARGV[9], ARGV[3]) ".
                "redis.call('HMSET', ARGV[1]..ARGV[9], ARGV[2], ARGV[10], ARGV[3], ARGV[11], ARGV[4], ARGV[12], ARGV[5], ARGV[13]) ".
                "if (ARGV[13] == '0') then ".
                "redis.call('EXPIRE', ARGV[1]..ARGV[9], ARGV[14]) ".
                "end ".
                "if next(KEYS) ~= nil then ".
                "redis.call('SADD', ARGV[6], unpack(KEYS)) ".
                "for _, tagname in ipairs(KEYS) do ".
                "redis.call('SADD', ARGV[7]..tagname, ARGV[9]) ".
                "end ".
                "end ".
                "if (ARGV[15] == '1') then ".
                "redis.call('SADD', ARGV[8], ARGV[9]) ".
                "end ".
                "if (oldTags ~= false) then ".
                "return oldTags ".
                "else ".
                "return '' ".
                "end"
            ],
        'removeByMatchingAnyTags' => [
            "sha1" => 'a6d92d0d20e5c8fa3d1a4cf7417191b66676ce43',
            "code" => "for i = 1, #KEYS, ARGV[6] do " .
                "local prefixedTags = {} " .
                "for x, tag in ipairs(KEYS) do " .
                "prefixedTags[x] = ARGV[1]..tag " .
                "end " .
                "local keysToDel = redis.call('SUNION', unpack(prefixedTags, i, math.min(#prefixedTags, i + ARGV[6] - 1))) " .
                "for _, keyname in ipairs(keysToDel) do " .
                "redis.call('DEL', ARGV[2]..keyname) " .
                "if (ARGV[5] == '1') then " .
                "redis.call('SREM', ARGV[4], keyname) " .
                "end " .
                "end " .
                "redis.call('DEL', unpack(prefixedTags, i, math.min(#prefixedTags, i + ARGV[6] - 1))) " .
                "redis.call('SREM', ARGV[3], unpack(KEYS, i, math.min(#KEYS, i + ARGV[6] - 1))) " .
                "end " .
                "return true"
            ],
        'collectGarbage' => [
            "sha1" => 'c00416b970f1aa6363b44965d4cf60ee99a6f065',
            "code" => "local tagKeys = {} ".
                "local expired = {} ".
                "local expiredCount = 0 ".
                "local notExpiredCount = 0 ".
                "for _, tagName in ipairs(KEYS) do ".
                "tagKeys = redis.call('SMEMBERS', ARGV[4]..tagName) ".
                "for __, keyName in ipairs(tagKeys) do ".
                "if (redis.call('EXISTS', ARGV[1]..keyName) == 0) then ".
                "expiredCount = expiredCount + 1 ".
                "expired[expiredCount] = keyName ".
                /* Redis Lua scripts have a hard limit of 8000 parameters per command */
                "if (expiredCount == 7990) then ".
                "redis.call('SREM', ARGV[4]..tagName, unpack(expired)) ".
                "if (ARGV[5] == '1') then ".
                "redis.call('SREM', ARGV[3], unpack(expired)) ".
                "end ".
                "expiredCount = 0 ".
                "expired = {} ".
                "end ".
                "else ".
                "notExpiredCount = notExpiredCount + 1 ".
                "end ".
                "end ".
                "if (expiredCount > 0) then ".
                "redis.call('SREM', ARGV[4]..tagName, unpack(expired)) ".
                "if (ARGV[5] == '1') then ".
                "redis.call('SREM', ARGV[3], unpack(expired)) ".
                "end ".
                "end ".
                "if (notExpiredCount == 0) then ".
                "redis.call ('DEL', ARGV[4]..tagName) ".
                "redis.call ('SREM', ARGV[2], tagName) ".
                "end ".
                "expired = {} ".
                "expiredCount = 0 ".
                "notExpiredCount = 0 ".
                "end ".
                "return true"
        ],
    ];

    /**
     * On large data sets SUNION slows down considerably when used with too many arguments
     * so this is used to chunk the SUNION into a few commands where the number of set ids
     * exceeds this setting.
     *
     */
    protected int $_sUnionChunkSize = 500;

    /**
     * Maximum number of ids to be removed at a time
     *
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
     * @throws Zend_Cache_Exception
     */
    public function __construct(array $options = [])
    {
        $clientFactory = new ClientFactory();
        $this->_client = $clientFactory->create($options);

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

        $this->setCompressionConfiguration($options);
    }

    /**
     * @inheritDoc
     */
    public function getIds()
    {
        if ($this->_notMatchingTags) {
            return $this->_client->smembers(self::SET_IDS);
        }

        $keys = $this->_client->keys(self::PREFIX_KEY . '*');
        $prefixLength = strlen(ClientFactory::DEFAULT_PREFIX . self::PREFIX_KEY);

        return array_map(static function ($key) use ($prefixLength) {
            return substr($key, $prefixLength);
        }, $keys);
    }

    /**
     * @inheritDoc
     */
    public function getTags()
    {
        return $this->_client->smembers(self::SET_TAGS);
    }

    /**
     * @inheritDoc
     */
    public function getIdsMatchingTags($tags = [])
    {
        if (empty($tags)) {
            return $tags;
        }

        return $this->_client->sinter($this->_processTagIds($tags));
    }

    /**
     * @inheritDoc
     */
    public function getIdsNotMatchingTags($tags = [])
    {
        if(!$this->_notMatchingTags) {
            Zend_Cache::throwException("notMatchingTags option is currently disabled.");
        }

        if (empty($tags)) {
            return $this->_client->smembers(self::SET_IDS);
        }

        return $this->_client->sdiff([self::SET_IDS, ...$this->_processTagIds($tags)]);
    }

    /**
     * @inheritDoc
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
     * @inheritDoc
     */
    public function getFillingPercentage()
    {
        $maxMemory = $this->_client->config('GET', 'maxmemory');

        if (0 === (int) $maxMemory['maxmemory']) {
            return 1;
        }

        $info = $this->_client->info('Memory');

        return (int) round(
            ((int) $info['Memory']['used_memory']/$maxMemory['maxmemory'] * 100)
        );
    }

    /**
     * @inheritDoc
     */
    public function getMetadatas($id)
    {
        [$tags, $mtime, $inf] = $this->_client->hmget(
            self::PREFIX_KEY . $id,
            self::FIELD_TAGS, self::FIELD_MTIME,
            self::FIELD_INF
        );

        if (!$mtime) {
            return false;
        }

        $tags = explode(',', $this->_decodeData($tags));
        $expire = $inf === '1' ? false : time() + $this->_client->ttl(self::PREFIX_KEY . $id);

        return [
            'expire' => $expire,
            'tags'   => $tags,
            'mtime'  => $mtime,
        ];
    }

    /**
     * @inheritDoc
     */
    public function touch($id, $extraLifetime)
    {
        [$inf] = $this->_client->hget(self::PREFIX_KEY . $id, self::FIELD_INF);

        if ($inf === '0') {
            $ttl = $this->_client->ttl(self::PREFIX_KEY . $id);
            $expire = $ttl + $extraLifetime;

            return (bool) $this->_client->expire(self::PREFIX_KEY . $id, $expire);
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function getCapabilities()
    {
        return array(
            'automatic_cleaning' => ($this->_automaticCleaningFactor > 0),
            'tags'               => true,
            'expired_read'       => false,
            'priority'           => false,
            'infinite_lifetime'  => true,
            'get_list'           => true,
        );
    }

    /**
     * @inheritDoc
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $data = $this->_client->hget(self::PREFIX_KEY . $id, self::FIELD_DATA);

        if (null === $data) {
            return false;
        }

        $decodedData = $this->_decodeData($data);

        if ($this->_autoExpireLifetime === 0 || !$this->_autoExpireRefreshOnLoad) {
            return $decodedData;
        }

        if (!$this->_matchesAutoExpiringPattern($id)) {
            return $decodedData;
        }

        $this->_client->expire(self::PREFIX_KEY . $id, min($this->_autoExpireLifetime, self::MAX_LIFETIME));

        return $decodedData;
    }

    /**
     * @param string $data
     * @param int $level
     * @return string
     */
    protected function _encodeData(string $data, int $level): string
    {
        if ($this->_compressionLib && $level !== 0 && strlen($data) >= $this->_compressThreshold) {
            $data = match ($this->_compressionLib) {
                'snappy' => snappy_compress($data),
                'lzf' => lzf_compress($data),
                'l4z' => lz4_compress($data, $level),
                'zstd' => zstd_compress($data, $level),
                'gzip' => gzcompress($data, $level),
                default => throw new \InvalidArgumentException("Unrecognized 'compression_lib'."),
            };

            if(!$data) {
                throw new RuntimeException("Could not compress cache data.");
            }
            return $this->_compressPrefix . $data;
        }

        return $data;
    }

    /**
     * @param bool|string $data
     * @return string
     */
    protected function _decodeData($data)
    {
        $actualData = substr($data,5);

        try {
            if (substr($data,2,3) === self::COMPRESS_PREFIX) {
                switch(substr($data,0,2)) {
                    case 'sn': return snappy_uncompress($actualData);
                    case 'lz': return lzf_decompress($actualData);
                    case 'l4': return lz4_uncompress($actualData);
                    case 'zs': return zstd_uncompress($actualData);
                    case 'gz': case 'zc': return gzuncompress($actualData);
                }
            }
        } catch(Exception $e) {
            $data = false;
        }

        return $data;
    }

    /**
     * @inheritDoc
     */
    public function test($id)
    {
        $mtime = $this->_client->hGet(self::PREFIX_KEY . $id, self::FIELD_MTIME);
        return ($mtime ?: FALSE);
    }

    /**
     * Get the lifetime
     *
     * if $specificLifetime is not false, the given specific life time is used
     * else, the global lifetime is used
     *
     * @param  int $specificLifetime
     * @return int Cache lifetime
     */
    public function getLifetime($specificLifetime)
    {
        // Lifetimes set via Layout XMLs get parsed as string so bool(false) becomes string("false")
        if ($specificLifetime === 'false') {
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
     * @param $lifetime
     * @param string $id
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
        return str_contains($id, "REQEST");
    }

    /**
     * @inheritDoc
     * @throws Zend_Cache_Exception
     */
    public function save($data, $id, $tags = [], $specificLifetime = false)
    {
        $lifetime = $this->_getAutoExpiringLifetime($this->getLifetime($specificLifetime), $id);
        $oldTags = $this->_client->hget(self::PREFIX_KEY . $id, self::FIELD_TAGS);
        $oldTags = (null !== $oldTags) ? explode(',', $oldTags) : [];

        if ($this->_useLua) {
            $result = $this->_executeLuaScript('save', $tags , [
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
                $this->_encodeData(implode(',',$tags), $this->_compressTags),
                time(),
                $lifetime ? 0 : 1,
                min($lifetime, self::MAX_LIFETIME),
                $this->_notMatchingTags ? 1 : 0
            ]);

            // Process removed tags if cache entry already existed
            if ($result) {
                $oldTags = explode(',', $this->_decodeData($result));
                if ($remTags = ($oldTags ? array_diff($oldTags, $tags) : false)) {
                    $this->_client->pipeline(function (Pipeline $pipeline) use ($remTags, $id) {
                        // Update the id list for each tag
                        foreach($remTags as $tag)
                        {
                            $pipeline->sRem(self::PREFIX_TAG_IDS . $tag, $id);
                        }
                    });
                }
            }

            return true;
        }

        $this->_client->pipeline(function (Pipeline $pipeline) use ($data, $id, $tags, $oldTags, $lifetime) {
            $pipeline->multi();

            $pipeline->hset(
                self::PREFIX_KEY . $id,
                self::FIELD_DATA,
                $this->_encodeData($data, $this->_compressData),
                self::FIELD_TAGS,
                $this->_encodeData(implode(',',$tags), $this->_compressTags),
                self::FIELD_MTIME,
                time(),
                self::FIELD_INF,
                is_null($lifetime) ? 1 : 0
            );

            if ($lifetime !== false && !is_null($lifetime)) {
                $pipeline->expire(self::PREFIX_KEY . $id, min($lifetime, self::MAX_LIFETIME));
            }

            if ($tags) {
                // Update the list with all the tags
                $pipeline->sadd( self::SET_TAGS, $tags);

                // Update the id list for each tag
                foreach($tags as $tag)
                {
                    $pipeline->sadd(self::PREFIX_TAG_IDS . $tag, $id);
                }
            }

            if ($remTags = ($oldTags ? array_diff($oldTags, $tags) : false)) {
                // Update the id list for each tag
                foreach($remTags as $tag)
                {
                    $pipeline->srem(self::PREFIX_TAG_IDS . $tag, $id);
                }
            }

            // Update the list with all the ids
            if ($this->_notMatchingTags) {
                $pipeline->sAdd(self::SET_IDS, $id);
            }

            $pipeline->exec();
        });

        return true;
    }

    /**
     * @inheritDoc
     */
    public function remove($id)
    {
        $tags = $this->_client->hget(self::PREFIX_KEY . $id, self::FIELD_TAGS);
        $tags = (null !== $tags) ? explode(',', $tags) : [];

        $this->_client->multi();

        $this->_client->del(self::PREFIX_KEY . $id);

        if ($this->_notMatchingTags) {
            $this->_client->srem(self::SET_IDS, $id);
        }

        foreach($tags as $tag) {
            $this->_client->srem(self::PREFIX_TAG_IDS . $tag, $id);
        }

        $result = $this->_client->exec();

        return (bool) $result[0];
    }

    /**
     * @inheritDoc
     * @throws Zend_Cache_Exception
     */
    public function clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array())
    {
        try {
            if (empty($tags)) {
                switch ($mode) {
                    case Zend_Cache::CLEANING_MODE_ALL:
                        $this->_client->flushdb();
                        return true;
                    case Zend_Cache::CLEANING_MODE_OLD:
                        $this->_collectGarbage();
                        return true;
                    default: return true;
                }
            }

            switch ($mode) {
                case Zend_Cache::CLEANING_MODE_MATCHING_TAG:
                    $this->_removeByMatchingTags($tags);
                    return true;
                case Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
                    $this->_removeByNotMatchingTags($tags);
                    return true;
                case Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
                    $this->_removeByMatchingAnyTags($tags);
                    return true;
                default:
                    Zend_Cache::throwException('Invalid mode for clean() method: ' . $mode);
            }
        } catch (Throwable $e) {
            Zend_Cache::throwException('Error cleaning cache on mode '.$mode.': '.$e->getMessage(), $e);
        }
    }

    /**
     * Removes all ids that matches given tags.
     *
     * @param array $tags
     * @return void
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
     *
     * @param array $tags
     * @return void
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
     * @param array $tags
     * @return void
     * @throws Zend_Cache_Exception
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
                    ($this->_notMatchingTags ? 1 : 0),
                    $this->_luaMaxCStack
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

            $pipeline->del( $this->_processTagIds($tags));
            $pipeline->sRem( self::SET_TAGS, ...$tags);

            $pipeline->exec();
        });
    }

    /**
     * Remove id keys.
     *
     * @param array $ids
     * @return void
     */
    private function _removeIds(array $ids): void
    {
        $ids = array_chunk($ids, $this->_removeChunkSize);

        $this->_client->pipeline(function (Pipeline $pipeline) use ($ids) {
            foreach ($ids as $chunk) {
                $pipeline->multi();

                $pipeline->del($this->_processIds($chunk));

                if($this->_notMatchingTags) {
                    $pipeline->srem( self::SET_IDS, ...$chunk);
                }

                $pipeline->exec();
            }
        });
    }

    /**
     * Apply prefix for given tags ids.
     *
     * @param array $tags
     * @return array
     */
    protected function _processTagIds(array $tags): array
    {
        array_walk($tags, [$this, '_applyPrefix'], self::PREFIX_TAG_IDS);
        return $tags;
    }

    /**
     * Apply prefix for given ids.
     *
     * @param array $ids
     * @return array
     */
    protected function _processIds(array $ids): array
    {
        array_walk($ids, [$this, '_applyPrefix'], self::PREFIX_KEY);
        return $ids;
    }

    /**
     * Callback that applies given prefix.
     *
     * @param $item
     * @param $index
     * @param $prefix
     */
    protected function _applyPrefix(&$item, $index, $prefix)
    {
        $item = $prefix . $item;
    }

    /**
     * Clean up expired tags and id's.
     *
     * @return void
     * @throws Zend_Cache_Exception
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
                    ($this->_notMatchingTags ? 1 : 0)
                ]);

                usleep(20000);
            }

            return;
        }

        $tags = $this->_client->smembers(self::SET_TAGS);

        foreach ($tags as $tag) {
            $tagMembers = $this->_client->smembers(self::PREFIX_TAG_IDS . $tag);
            $expiredMembersCount = 0;

            if (!empty($tagMembers)) {
                $existingStatuses = $this->_client->pipeline(function (Pipeline $pipeline) use ($tagMembers) {
                    foreach ($tagMembers as $member) {
                        $pipeline->exists(self::PREFIX_KEY . $member);
                    }
                });

                $expiredMembers = array_udiff($tagMembers, $existingStatuses, static function ($member, $isExpired) {
                    return ($isExpired === 0) ? 1 : -1;
                });

                $expiredMembersCount = count($expiredMembers);

                $this->_client->pipeline(function (Pipeline $pipeline) use ($expiredMembers, $tag) {
                    foreach ($expiredMembers as $member) {
                        $pipeline->srem(self::PREFIX_TAG_IDS . $tag, $member);

                        if ($this->_notMatchingTags) {
                            $pipeline->srem(self::SET_IDS, $member);
                        }
                    }
                });
            }

            if ($expiredMembersCount === count($tagMembers)) {
                $this->_client->del(self::PREFIX_TAG_IDS . $tag);
                $this->_client->srem(self::SET_TAGS, $tag);
            }
        }
    }

    /**
     * Set compression configuration from given options.
     *
     * @param array $options
     * @return void
     */
    public function setCompressionConfiguration(array $options): void
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
            if (function_exists($library . "_compress")) {
                $existingLibName = $library;
                break;
            }
        }

        if ($existingLibName === '') {
            $existingLibName = 'gzip';
        }

        switch ($existingLibName) {
            case 'lz4':
                if ($this->_isSatisfiedCompressionLibVersion($existingLibName, "0.3.0")) {
                    $this->_compressTags = $this->_compressTags > 1;
                    $this->_compressData = $this->_compressData > 1;
                }

                break;
            case 'zstd':
                if ($this->_isSatisfiedCompressionLibVersion($existingLibName, "0.4.13")) {
                    $this->_compressTags = $this->_compressTags > 1;
                    $this->_compressData = $this->_compressData > 1;
                }

                break;
        }

        $this->_compressionLib = $existingLibName;
        $this->_compressPrefix = substr($this->_compressionLib,0,2) . self::COMPRESS_PREFIX;
    }

    /**
     * Checks whether actual version of given compression library is lower than required.
     *
     * @param string $compressionLib
     * @param string $version
     * @return bool
     */
    private function _isSatisfiedCompressionLibVersion(string $compressionLib, string $version): bool
    {
        $actualVersion = phpversion($compressionLib);

        return version_compare($version, $actualVersion) < 0;
    }

    /**
     * Executes given LUA script with provided arguments.
     *
     * @param string $scriptName
     * @param array $keys
     * @param array $arguments
     * @return mixed
     * @throws Zend_Cache_Exception
     */
    protected function _executeLuaScript(string $scriptName, array $keys = [], array $arguments = []): mixed
    {
        if (!array_key_exists($scriptName, $this->_luaScripts)) {
            Zend_Cache::throwException('Non existing script with following name.');
        }

        $redisVersion = $this->_getRedisServerVersion();

        if (version_compare($redisVersion, '7.0.0') >= 0) {
            $this->_loadRedisFunction($scriptName);

            return $this->_client->fcall($scriptName, $keys, ...$arguments);
        }

        $result =
            $this->_client->evalsha($this->_luaScripts[$scriptName]['sha1'], count($keys), ...$keys, ...$arguments);

        if (is_null($result)) {
            return $this->_client->eval($this->_luaScripts[$scriptName]['code'], count($keys), ...$keys, ...$arguments);
        }

        return $result;
    }

    /**
     * @return string
     */
    private function _getRedisServerVersion(): string
    {
        $info = $this->_client->info();

        return $info['Server']['redis_version'];
    }

    /**
     * Loads given LUA script as redis function.
     *
     * @param string $scriptName
     * @return void
     * @throws Zend_Cache_Exception
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
        $function .= $processedFunctionCode . ' end)';

        $this->_client->function->load($function);
    }
}
