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

use Assert\Assertion;
use Broadway\Saga\State\RepositoryInterface;
use Broadway\Saga\State\Testing\AbstractRepositoryTest;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\StringType;

class DbalRepositoryTest extends AbstractRepositoryTest
{
    /** @var Connection */
    protected $connection;

    protected $tableName = 'saga_state';

    public function setUp(): void
    {
        $this->connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);

        $this->createTable();

        parent::setUp();
    }

    /**
     * {@inheritdoc}
     */
    protected function createRepository(): RepositoryInterface
    {
        return new DbalRepository($this->connection, 'saga_state');
    }

    protected function createTable(): void
    {
        /** @var AbstractSchemaManager $schemaManager */
        $schemaManager = $this->connection->getSchemaManager();

        /** @var Table $table */
        $table = $schemaManager->createSchema()->createTable($this->tableName);

        $table->addColumn('id', 'guid', ['length' => 36]);
        $table->addColumn('done', 'boolean', ['default' => false]);
        $table->addColumn('sagaId', 'string');
        $table->setPrimaryKey(['id']);

        $schemaManager->createTable($table);
    }

    /**
     * {@inheritdoc}
     */
    public function tearDown(): void
    {
        $this->connection->getSchemaManager()->dropTable($this->tableName);
    }

    /**
     * @test
     */
    public function it_saves_a_state(): void
    {
        $this->addColumns([
            new Column('appId', new IntegerType(), [
                'unsigned' => true,
                'default' => 0,
            ]),
        ]);

        parent::it_saves_a_state();
    }

    /**
     * @test
     */
    public function it_removes_a_state_when_state_is_done(): void
    {
        $this->addColumns([
            new Column('appId', new IntegerType(), [
                'unsigned' => true,
                'default' => 0,
            ]),
        ]);

        parent::it_removes_a_state_when_state_is_done();
    }

    /**
     * @test
     */
    public function it_finds_documents_matching_criteria(): void
    {
        $this->addColumns([
            new Column('Hi', new StringType(), ['notnull' => true, 'default' => '']),
            new Column('Bye', new StringType(), ['notnull' => true, 'default' => '']),
            new Column('You', new StringType(), ['notnull' => true, 'default' => '']),
        ]);

        parent::it_finds_documents_matching_criteria();
    }

    /**
     * @test
     */
    public function it_finds_documents_matching_in_criteria(): void
    {
        $this->markTestSkipped('cannot store nested values');
    }

    /**
     * @test
     */
    public function it_returns_null_when_no_states_match_the_criteria(): void
    {
        $this->addColumns([
            new Column('Hi', new StringType(), ['notnull' => true, 'default' => '']),
            new Column('Bye', new StringType(), ['notnull' => true, 'default' => '']),
        ]);

        parent::it_returns_null_when_no_states_match_the_criteria();
    }

    /**
     * @test
     */
    public function saving_a_state_object_with_the_same_id_only_keeps_the_last_one(): void
    {
        $this->addColumns([
            new Column('appId', new IntegerType(), [
                'unsigned' => true,
                'default' => 0,
            ]),
        ]);

        parent::saving_a_state_object_with_the_same_id_only_keeps_the_last_one();
    }

    /**
     * @test
     */
    public function it_throws_an_exception_if_multiple_matching_elements_are_found(): void
    {
        $this->addColumns([
            new Column('appId', new IntegerType(), [
                'unsigned' => true,
                'default' => 0,
            ]),
        ]);

        parent::it_throws_an_exception_if_multiple_matching_elements_are_found();
    }

    /**
     * @param Column[] $columns
     */
    protected function addColumns(array $columns)
    {
        Assertion::allIsInstanceOf($columns, Column::class);
        $this->connection->getSchemaManager()->alterTable(new TableDiff($this->tableName, $columns));
    }
}
