<?php

$finder = PhpCsFixer\Finder::create()
    ->exclude(['build', 'vendor'])
    ->in(__DIR__);

return (new PhpCsFixer\Config())
    ->setFinder($finder)
    ->setUsingCache(true)
    ->setRules([
        '@Symfony' => true,
        'phpdoc_align' => false,
        'phpdoc_order' => true,
        'ordered_imports' => true,
        'array_syntax' => ['syntax' => 'short'],
    ]);
