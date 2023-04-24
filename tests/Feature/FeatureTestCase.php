<?php

namespace Redis\Pmc\Feature;

use PHPUnit\Framework\TestCase;
use Predis\Client;
use Redis\Pmc\Constraints\ArrayHasSameValuesConstraint;

abstract class FeatureTestCase extends TestCase
{
    /**
     * Returns a named array with default values for connection parameters.
     *
     * @return array Default connection parameters
     */
    protected function getDefaultParametersArray(): array
    {
        return [
            'scheme' => 'tcp',
            'host' => constant('REDIS_SERVER_HOST'),
            'port' => constant('REDIS_SERVER_PORT'),
            'database' => constant('REDIS_SERVER_DBNUM'),
        ];
    }

    /**
     * Creates Redis client with default or given configuration.
     *
     * @param array|null $parameters
     * @param array|null $options
     * @param bool|null $flushDB
     * @return Client
     */
    protected function getClient(?array $parameters = null, ?array $options = null, ?bool $flushDB = true): Client
    {
        $parameters = array_merge(
            $this->getDefaultParametersArray(),
            $parameters ?: []
        );

        $client = new Client($parameters, $options);

        if ($flushDB) {
            $client->flushdb();
        }

        return $client;
    }

    /**
     * Asserts that two arrays have the same values (even with different order).
     *
     * @param array  $expected Expected array
     * @param array  $actual   Actual array
     * @param string $message  Optional assertion message
     */
    public function assertSameValues(array $expected, array $actual, $message = ''): void
    {
        $this->assertThat($actual, new ArrayHasSameValuesConstraint($expected), $message);
    }
}
