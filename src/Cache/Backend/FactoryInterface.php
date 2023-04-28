<?php

namespace Redis\Pmc\Cache\Backend;

use Predis\Client;
use Predis\ClientInterface;

interface FactoryInterface
{
    /**
     * Creates instance of Predis client.
     *
     * @param array $options
     * @return Client
     */
    public function create(array $options): ClientInterface;
}
