<?php

namespace Redis\Pmc\Constraints;

use PHPUnit\Framework\Constraint\Constraint;

/**
 * PHPUnit's constraint matching arrays with same elements even in different order.
 */
class ArrayHasSameValuesConstraint extends Constraint
{
    protected array $array;

    /**
     * @param array $array
     */
    public function __construct(array $array)
    {
        $this->array = $array;
    }

    /**
     * {@inheritdoc}
     */
    public function matches($other): bool
    {
        if (count($this->array) !== count($other)) {
            return false;
        }

        if (array_diff($this->array, $other)) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function toString(): string
    {
        return 'two arrays contain the same elements.';
    }

    /**
     * {@inheritdoc}
     */
    protected function failureDescription($other): string
    {
        return $this->toString();
    }
}
