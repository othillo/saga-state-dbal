<?php

declare(strict_types=1);

/*
 * This file is part of the broadway/broadway-saga package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\Saga\State\Dbal;

use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryException;
use Broadway\Saga\State\RepositoryInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\ResultStatement;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Query\QueryBuilder;

class DbalRepository implements RepositoryInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $tableName;

    public function __construct(Connection $connection, string $tableName)
    {
        $this->connection = $connection;
        $this->tableName = $tableName;
    }

    /**
     * {@inheritdoc}
     */
    public function findOneBy(Criteria $criteria, string $sagaId): ?State
    {
        $results = $this->createAndExecuteQuery($criteria, $sagaId)
            ->fetchAll();

        if (1 === count($results)) {
            $result = current($results);

            $values = $result;
            unset($values['id']);
            unset($values['done']);
            unset($values['sagaId']);

            return State::deserialize([
                'id' => $result['id'],
                'done' => (bool) $result['done'],
                'values' => $values,
            ]);
        }

        if (count($results) > 1) {
            throw new RepositoryException('Multiple saga state instances found.');
        }

        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function save(State $state, string $sagaId): void
    {
        $this->connection->beginTransaction();

        try {
            $this->connection->insert($this->tableName, array_merge([
                'id' => $state->getId(),
                'done' => $state->isDone(),
                'sagaId' => $sagaId,
            ], $state->getValues()));

            $this->connection->commit();
        } catch (UniqueConstraintViolationException $e) {
            $this->connection->update($this->tableName, array_merge([
                'done' => $state->isDone(),
                'sagaId' => $sagaId,
            ], $state->getValues()), [
                'id' => $state->getId(),
            ]);
        } catch (\Throwable $e) {
            $this->connection->rollBack();

            throw $e;
        }
    }

    protected function createAndExecuteQuery(Criteria $criteria, string $sagaId): ResultStatement
    {
        /** @var QueryBuilder $queryBuilder */
        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->tableName)
            ->where('done = :done')
            ->setParameter(':done', false);

        foreach ($criteria->getComparisons() as $key => $value) {
            $queryBuilder->andWhere(sprintf('%s = :%s', $key, $key));
            $queryBuilder->setParameter(sprintf(':%s', $key), $value);
        }

        return $queryBuilder->execute();
    }
}
