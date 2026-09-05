#!/usr/bin/env python3
"""
Generate realistic test data for pipeline validation
"""

from __future__ import annotations

import os
import random
import string
from datetime import datetime, timedelta

import mysql.connector
import psycopg2


# No faker dependency - use simple generation
def fake_name() -> str:
    first = random.choice(['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana'])
    last = random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Davis'])
    return f"{first} {last}"


def fake_email(name: str) -> str:
    domain = random.choice(['example.com', 'test.com', 'demo.com'])
    return f"{name.lower().replace(' ', '.')}@{domain}"


def fake_word() -> str:
    words = ['Widget', 'Gadget', 'Device', 'Tool', 'Item', 'Product', 'Thing']
    return random.choice(words)


def fake_sentence() -> str:
    subjects = ['The user', 'The system', 'The process', 'The transaction']
    verbs = ['completed', 'initiated', 'processed', 'verified']
    objects = ['successfully', 'with errors', 'as expected', 'normally']
    return f"{random.choice(subjects)} {random.choice(verbs)} {random.choice(objects)}"


class TestDataGenerator:

    def __init__(self):
        self.mysql_conn = None
        self.postgres_conn = None

        # Defaults match current docker compose setup
        self.mysql_host = os.getenv('MYSQL_HOST', 'localhost')
        self.mysql_port = int(os.getenv('MYSQL_PORT', '3307'))  # docker-compose.e2e.yml
        self.mysql_user = os.getenv('MYSQL_USER', 'root')
        self.mysql_password = os.getenv('MYSQL_PASSWORD', 'rootpassword')
        self.mysql_db = os.getenv('MYSQL_DB', 'test_db')

        self.pg_host = os.getenv('PG_HOST', 'localhost')
        self.pg_port = int(os.getenv('PG_PORT', '5432'))  # docker-compose.yml
        self.pg_user = os.getenv('PG_USER', 'user')
        self.pg_password = os.getenv('PG_PASSWORD', 'password')
        self.pg_db = os.getenv('PG_DB', 'test_db')
        self.pg_admin_db = os.getenv('PG_ADMIN_DB', 'pipeline_db')

    def connect_mysql(self):
        """Connect to MySQL running in Docker"""
        print('Connecting to MySQL...')
        try:
            try:
                self.mysql_conn = mysql.connector.connect(
                    host=self.mysql_host,
                    port=self.mysql_port,
                    user=self.mysql_user,
                    password=self.mysql_password,
                    database=self.mysql_db,
                )
                self.mysql_conn.autocommit = False
            except mysql.connector.errors.DatabaseError as e:
                if 'Unknown database' in str(e) or '1049' in str(e):
                    root_conn = mysql.connector.connect(
                        host=self.mysql_host,
                        port=self.mysql_port,
                        user=self.mysql_user,
                        password=self.mysql_password,
                    )
                    root_conn.autocommit = True
                    cur = root_conn.cursor()
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_db}")
                    cur.close()
                    root_conn.close()

                    self.mysql_conn = mysql.connector.connect(
                        host=self.mysql_host,
                        port=self.mysql_port,
                        user=self.mysql_user,
                        password=self.mysql_password,
                        database=self.mysql_db,
                    )
                    self.mysql_conn.autocommit = False
                else:
                    raise

            print('✅ MySQL connected')
            return self.mysql_conn.cursor()
        except Exception as e:
            print(f"❌ MySQL connection failed: {e}")
            print('   Ensure MySQL is running (overlay):')
            print('   docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d mysql-e2e')
            raise

    def connect_postgres(self):
        """Connect to PostgreSQL running in Docker"""
        print('Connecting to PostgreSQL...')
        try:
            admin = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                database=self.pg_admin_db,
            )
            admin.autocommit = True
            cur = admin.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.pg_db,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{self.pg_db}"')
            cur.close()
            admin.close()

            self.postgres_conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                database=self.pg_db,
            )
            self.postgres_conn.autocommit = False
            print('✅ PostgreSQL connected')
            return self.postgres_conn.cursor()
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            print('   Ensure Postgres is running: docker compose up -d postgres')
            raise

    def create_mysql_test_tables(self):
        """Create tables with various data types"""
        print('\nCreating MySQL test tables...')
        cursor = self.connect_mysql()

        cursor.execute('DROP TABLE IF EXISTS small_table')
        cursor.execute('DROP TABLE IF EXISTS medium_table')
        cursor.execute('DROP TABLE IF EXISTS large_table')

        cursor.execute('''
            CREATE TABLE small_table (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP,
                score DECIMAL(10,2)
            )
        ''' )
        print('  ✅ small_table created')

        cursor.execute('''
            CREATE TABLE medium_table (
                id INT PRIMARY KEY,
                user_id INT,
                product_name VARCHAR(200),
                quantity INT,
                price DECIMAL(10,2),
                order_date DATE,
                status VARCHAR(50)
            )
        ''' )
        print('  ✅ medium_table created')

        cursor.execute('''
            CREATE TABLE large_table (
                id INT PRIMARY KEY,
                transaction_id VARCHAR(50),
                customer_id INT,
                amount DECIMAL(12,2),
                currency VARCHAR(3),
                timestamp TIMESTAMP,
                description TEXT
            )
        ''' )
        print('  ✅ large_table created')

        self.mysql_conn.commit()

    def populate_small_table(self):
        """100 rows - tests basic pipeline"""
        print('\nPopulating small_table (100 rows)...')
        cursor = self.mysql_conn.cursor()

        values = []
        for i in range(1, 101):
            name = fake_name()
            values.append((
                i,
                name,
                fake_email(name),
                datetime.now() - timedelta(days=random.randint(0, 365)),
                round(random.uniform(0, 100), 2),
            ))

        cursor.executemany('''
            INSERT INTO small_table (id, name, email, created_at, score)
            VALUES (%s, %s, %s, %s, %s)
        ''', values)
        self.mysql_conn.commit()
        print('  ✅ 100 rows inserted')

    def populate_medium_table(self):
        """10K rows - tests batch processing"""
        print('\nPopulating medium_table (10,000 rows)...')
        cursor = self.mysql_conn.cursor()

        batch_size = 1000
        for batch in range(10):
            values = []
            for i in range(batch * batch_size + 1, (batch + 1) * batch_size + 1):
                values.append((
                    i,
                    random.randint(1, 1000),
                    f"{fake_word()} {fake_word()}",
                    random.randint(1, 100),
                    round(random.uniform(10, 1000), 2),
                    datetime.now().date() - timedelta(days=random.randint(0, 730)),
                    random.choice(['pending', 'completed', 'cancelled', 'shipped']),
                ))

            cursor.executemany('''
                INSERT INTO medium_table
                (id, user_id, product_name, quantity, price, order_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', values)
            print(f"  Batch {batch + 1}/10 ({(batch + 1) * batch_size:,} rows)", end="\r")

        self.mysql_conn.commit()
        print('\n  ✅ 10,000 rows inserted')

    def populate_large_table(self):
        """100K rows - tests scale"""
        print('\nPopulating large_table (100,000 rows)...')
        print('  This will take 1-2 minutes...')
        cursor = self.mysql_conn.cursor()

        batch_size = 5000
        for batch in range(20):
            values = []
            for i in range(batch * batch_size + 1, (batch + 1) * batch_size + 1):
                tx_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
                values.append((
                    i,
                    tx_id,
                    random.randint(1, 10000),
                    round(random.uniform(1, 10000), 2),
                    random.choice(['USD', 'EUR', 'GBP', 'JPY']),
                    datetime.now() - timedelta(seconds=random.randint(0, 31536000)),
                    fake_sentence(),
                ))

            cursor.executemany('''
                INSERT INTO large_table
                (id, transaction_id, customer_id, amount, currency, timestamp, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', values)
            progress = (batch + 1) * batch_size
            print(f"  Progress: {progress:,}/100,000 rows ({(batch + 1) * 5}%)", end="\r")

        self.mysql_conn.commit()
        print('\n  ✅ 100,000 rows inserted')

    def create_postgres_destination_tables(self):
        """Create matching tables in PostgreSQL"""
        print('\nCreating PostgreSQL destination tables...')
        cursor = self.connect_postgres()

        cursor.execute('DROP TABLE IF EXISTS small_table_dest CASCADE')
        cursor.execute('DROP TABLE IF EXISTS medium_table_dest CASCADE')
        cursor.execute('DROP TABLE IF EXISTS large_table_dest CASCADE')

        cursor.execute('''
            CREATE TABLE small_table_dest (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP,
                score DECIMAL(10,2)
            )
        ''' )
        print('  ✅ small_table_dest created')

        cursor.execute('''
            CREATE TABLE medium_table_dest (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_name VARCHAR(200),
                quantity INTEGER,
                price DECIMAL(10,2),
                order_date DATE,
                status VARCHAR(50)
            )
        ''' )
        print('  ✅ medium_table_dest created')

        cursor.execute('''
            CREATE TABLE large_table_dest (
                id INTEGER PRIMARY KEY,
                transaction_id VARCHAR(50),
                customer_id INTEGER,
                amount DECIMAL(12,2),
                currency VARCHAR(3),
                timestamp TIMESTAMP,
                description TEXT
            )
        ''' )
        print('  ✅ large_table_dest created')

        self.postgres_conn.commit()

    def verify_data(self):
        """Verify source data exists"""
        print('\n' + '=' * 50)
        print('Data Verification:')
        print('=' * 50)

        cursor = self.mysql_conn.cursor()
        tables = [(
            'small_table', 100),
            ('medium_table', 10000),
            ('large_table', 100000),
        ]

        total_rows = 0
        for table, expected in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_rows += count
            status = "✅" if count == expected else "⚠️"
            print(f"  {status} {table}: {count:,} rows (expected {expected:,})")

        print(f"\nTotal rows generated: {total_rows:,}")

        print('\nSample data (first 3 rows from small_table):')
        cursor.execute('SELECT id, name, email, score FROM small_table LIMIT 3')
        rows = cursor.fetchall()
        for row in rows:
            print(f"  ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Score: {row[3]}")

    def cleanup(self):
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.postgres_conn:
            self.postgres_conn.close()


def main():
    print('╔════════════════════════════════════════════════════╗')
    print('║  Test Data Generator                              ║')
    print('╚════════════════════════════════════════════════════╝')
    print()
    print('This will generate:')
    print('  • 100 rows in small_table (MySQL)')
    print('  • 10,000 rows in medium_table (MySQL)')
    print('  • 100,000 rows in large_table (MySQL)')
    print('  • Matching destination tables (PostgreSQL)')
    print()

    gen = TestDataGenerator()

    try:
        gen.create_mysql_test_tables()
        gen.populate_small_table()
        gen.populate_medium_table()
        gen.populate_large_table()

        gen.create_postgres_destination_tables()
        gen.verify_data()

        print('')
        print('╔════════════════════════════════════════════════════╗')
        print('║  ✅ TEST DATA GENERATION COMPLETE                 ║')
        print('╚════════════════════════════════════════════════════╝')
        print()
        print('Next steps:')
        print('  1. Run pipeline tests: python3 tests/test_real_pipelines.py')
        print()

    except Exception as e:
        print()
        print('╔════════════════════════════════════════════════════╗')
        print('║  ❌ ERROR                                         ║')
        print('╚════════════════════════════════════════════════════╝')
        print(f"\n{e}\n")
        print('Troubleshooting:')
        print('  • Check if databases are running: docker compose ps')
        print('  • Start MySQL overlay (if needed):')
        print('      docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d mysql-e2e')

    finally:
        gen.cleanup()


if __name__ == '__main__':
    main()
