package validators

import (
	"strings"
	"testing"

	"api-gateway/internal/security"
)

// What this parser feeds is the list of tables a statement model is offered as the
// producer of. An invented target offers a schedule an upstream that never touches the
// table, so every "names nothing" case below carries a control: the nearest SQL that
// DOES name a target. Without it, an extractor that returned nothing for everything
// would pass every negative case.

func writeTargetNames(sql string) []string {
	return qualifiedNames(ExtractWriteTargets(sql))
}

func assertWriteTargets(t *testing.T, sql string, want ...string) {
	t.Helper()
	got := writeTargetNames(sql)
	if strings.Join(got, "|") != strings.Join(want, "|") || len(got) != len(want) {
		t.Fatalf("ExtractWriteTargets(%q)\n got: %q\nwant: %q", sql, got, want)
	}
}

type writeTargetCase struct {
	name string
	sql  string
	want []string
}

func runWriteTargetCases(t *testing.T, cases []writeTargetCase) {
	t.Helper()
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.want) == 0 {
				t.Fatal("a positive case must expect at least one target")
			}
			assertWriteTargets(t, tc.sql, tc.want...)
		})
	}
}

func TestExtractWriteTargets_TheStatementsThatLeaveRows(t *testing.T) {
	runWriteTargetCases(t, []writeTargetCase{
		{"insert select", "INSERT INTO analytics.daily SELECT * FROM raw.orders", []string{"analytics.daily"}},
		{"lowercase, bare name", "insert into orders values (1)", []string{"orders"}},
		{"column list", "INSERT INTO analytics.daily (id, total) SELECT id, total FROM raw.orders", []string{"analytics.daily"}},
		{"column list without a space", "INSERT INTO daily(id,total) VALUES (1, 2)", []string{"daily"}},
		{"parenthesized query", "INSERT INTO daily (SELECT * FROM raw.orders)", []string{"daily"}},
		{
			"parenthesized query with a CTE",
			"INSERT INTO analytics.daily (WITH s AS (SELECT 1 AS id) SELECT id FROM s)",
			[]string{"analytics.daily"},
		},
		{"three parts", "INSERT INTO warehouse.analytics.daily SELECT 1", []string{"warehouse.analytics.daily"}},
		{"mysql modifiers", "INSERT LOW_PRIORITY IGNORE INTO shop.orders SELECT * FROM staging", []string{"shop.orders"}},
		{"mysql DELAYED", "INSERT DELAYED INTO shop.orders VALUES (1)", []string{"shop.orders"}},
		{"mysql HIGH_PRIORITY", "INSERT HIGH_PRIORITY INTO shop.orders SELECT 1", []string{"shop.orders"}},
		{"mysql without INTO", "INSERT shop.orders VALUES (1)", []string{"shop.orders"}},
		{"mysql replace", "REPLACE INTO shop.orders SELECT * FROM staging", []string{"shop.orders"}},
		{"t-sql TOP", "INSERT TOP (10) PERCENT INTO dbo.orders SELECT * FROM staging", []string{"dbo.orders"}},
		{"t-sql EXEC source", "INSERT INTO dbo.orders (id) EXEC load_orders", []string{"dbo.orders"}},
		{"insert overwrite into", "INSERT OVERWRITE INTO analytics.daily SELECT * FROM raw.orders", []string{"analytics.daily"}},
		{
			"insert overwrite table, one partition",
			"INSERT OVERWRITE TABLE analytics.daily PARTITION (dt = '2026-09-01') SELECT * FROM raw.orders",
			[]string{"analytics.daily"},
		},
		{"insert into table", "INSERT INTO TABLE analytics.daily SELECT * FROM raw.orders", []string{"analytics.daily"}},
		{
			"ON CONFLICT DO UPDATE is still the insert",
			"INSERT INTO a.t VALUES (1) ON CONFLICT (id) DO UPDATE SET x = excluded.x",
			[]string{"a.t"},
		},
		{"update", "UPDATE analytics.orders SET status = 'done' WHERE id = 1", []string{"analytics.orders"}},
		{"update with alias", "UPDATE analytics.orders AS o SET x = 1", []string{"analytics.orders"}},
		{"update ONLY", "UPDATE ONLY analytics.orders SET x = 1", []string{"analytics.orders"}},
		{"mysql update LOW_PRIORITY", "UPDATE LOW_PRIORITY shop.orders SET x = 1", []string{"shop.orders"}},
		{"mysql update IGNORE", "UPDATE IGNORE shop.orders SET x = 1", []string{"shop.orders"}},
		{"update t-sql TOP", "UPDATE TOP (100) dbo.orders SET x = 1", []string{"dbo.orders"}},
		{"update t-sql hints", "UPDATE dbo.orders WITH (ROWLOCK) SET x = 1", []string{"dbo.orders"}},
		{
			"merge",
			`MERGE INTO analytics.customers AS c USING staging.customers s ON c.id = s.id
			 WHEN MATCHED THEN UPDATE SET name = s.name
			 WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)`,
			[]string{"analytics.customers"},
		},
		{
			"t-sql merge without INTO",
			"MERGE dbo.customers WITH (HOLDLOCK) AS c USING staging s ON 1 = 1 WHEN MATCHED THEN DELETE;",
			[]string{"dbo.customers"},
		},
		{"t-sql merge TOP", "MERGE TOP (5) INTO dbo.customers USING s ON 1 = 1 WHEN MATCHED THEN DELETE", []string{"dbo.customers"}},
		{"merge ONLY", "MERGE INTO ONLY analytics.customers c USING s ON TRUE WHEN MATCHED THEN DELETE", []string{"analytics.customers"}},
		{"create table as", "CREATE TABLE analytics.daily AS SELECT * FROM raw.orders", []string{"analytics.daily"}},
		{"unlogged, if not exists", "CREATE UNLOGGED TABLE IF NOT EXISTS analytics.daily AS SELECT 1", []string{"analytics.daily"}},
		{"or replace table", "CREATE OR REPLACE TABLE analytics.daily AS SELECT 1", []string{"analytics.daily"}},
		{"mysql create ... select", "CREATE TABLE shop.daily SELECT * FROM shop.orders", []string{"shop.daily"}},
		{"create table column list as", "CREATE TABLE analytics.daily (id, total) AS SELECT id, total FROM raw.orders", []string{"analytics.daily"}},
		{"create table as (query)", "CREATE TABLE analytics.daily AS (SELECT 1)", []string{"analytics.daily"}},
		{"create table as with", "CREATE TABLE analytics.daily AS WITH s AS (SELECT 1) SELECT * FROM s", []string{"analytics.daily"}},
		{"create table as values", "CREATE TABLE analytics.daily AS VALUES (1), (2)", []string{"analytics.daily"}},
		{"create table as table", "CREATE TABLE analytics.copy AS TABLE analytics.daily", []string{"analytics.copy"}},
		{"create table as execute", "CREATE TABLE analytics.copy AS EXECUTE daily_plan", []string{"analytics.copy"}},
		{"view", "CREATE OR REPLACE VIEW analytics.v AS SELECT * FROM raw.orders", []string{"analytics.v"}},
		{"t-sql or alter view", "CREATE OR ALTER VIEW dbo.v WITH SCHEMABINDING AS SELECT id FROM dbo.orders", []string{"dbo.v"}},
		{"materialized view", "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv AS SELECT 1", []string{"analytics.mv"}},
		{"refresh materialized view", "REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv", []string{"analytics.mv"}},
		{"trailing semicolon", "INSERT INTO a.t SELECT 1;", []string{"a.t"}},
		{"leading comment", "-- nightly load\nINSERT INTO a.t SELECT 1", []string{"a.t"}},
		{"leading block comment", "/* nightly */ UPDATE a.t SET x = 1", []string{"a.t"}},
	})
}

func TestExtractWriteTargets_QuotedIdentifiers(t *testing.T) {
	runWriteTargetCases(t, []writeTargetCase{
		{"double quotes", `INSERT INTO "Analytics"."Daily Orders" SELECT 1`, []string{"Analytics.Daily Orders"}},
		{"backticks", "INSERT INTO `shop`.`order items` SELECT 1", []string{"shop.order items"}},
		{"brackets", "UPDATE [dbo].[Order Lines] SET qty = 1", []string{"dbo.Order Lines"}},
		{"a quoted keyword is a name", `INSERT INTO "select" VALUES (1)`, []string{"select"}},
		{"a quoted keyword after a dot is a name", `INSERT INTO warehouse."select" SELECT 1`, []string{"warehouse.select"}},
		// A quoted word the verb would otherwise skip past is the table's name.
		{"a quoted INTO is a name", `INSERT INTO "into" SELECT 1`, []string{"into"}},
		{"a quoted ONLY is a name", `UPDATE "only" SET x = 1`, []string{"only"}},
		{"a quoted ONLY after MERGE INTO is a name", `MERGE INTO "only" USING s ON TRUE WHEN MATCHED THEN DELETE`, []string{"only"}},
		{"a dot inside quotes is not a separator", `INSERT INTO "a.b" VALUES (1)`, []string{"a.b"}},
		{"case is kept as written", "insert into Analytics.Orders select 1", []string{"Analytics.Orders"}},
		{"doubled quote inside a name", `MERGE INTO "we""ird" USING s ON TRUE WHEN MATCHED THEN DELETE`, []string{`we"ird`}},
	})

	quoted := ExtractWriteTargets(`INSERT INTO "Analytics"."Daily Orders" SELECT 1`)
	if len(quoted) != 1 || len(quoted[0].Parts) != 2 || !quoted[0].Quoted {
		t.Fatalf("want one two-part quoted ref, got %+v", quoted)
	}
	dotted := ExtractWriteTargets(`INSERT INTO "a.b" VALUES (1)`)
	if len(dotted) != 1 || len(dotted[0].Parts) != 1 {
		t.Fatalf("want one single-part ref, got %+v", dotted)
	}
	bare := ExtractWriteTargets("INSERT INTO analytics.orders SELECT 1")
	if len(bare) != 1 || bare[0].Quoted {
		t.Fatalf("want one unquoted ref, got %+v", bare)
	}
}

func TestExtractWriteTargets_CTEs(t *testing.T) {
	runWriteTargetCases(t, []writeTargetCase{
		{
			"a CTE before INSERT",
			`WITH recent AS (SELECT * FROM raw.orders WHERE ts > now() - interval '1 day')
			 INSERT INTO analytics.daily SELECT * FROM recent`,
			[]string{"analytics.daily"},
		},
		{
			"recursive, a column list, several CTEs, a materialization hint",
			`WITH RECURSIVE a (n) AS (SELECT 1), b AS NOT MATERIALIZED (SELECT * FROM a)
			 INSERT INTO analytics.t SELECT * FROM b`,
			[]string{"analytics.t"},
		},
		{
			"a CTE before MERGE",
			"WITH src AS (SELECT * FROM staging.c) MERGE INTO analytics.c USING src ON TRUE WHEN MATCHED THEN DELETE",
			[]string{"analytics.c"},
		},
		{
			"a CTE before UPDATE",
			"WITH s AS (SELECT id FROM staging) UPDATE analytics.orders SET x = 1 WHERE id IN (SELECT id FROM s)",
			[]string{"analytics.orders"},
		},
		{
			"a data-modifying CTE writes from inside its body",
			"WITH ins AS (INSERT INTO audit.log (id) VALUES (1) RETURNING id) SELECT * FROM ins",
			[]string{"audit.log"},
		},
		{
			"a moving CTE: the DELETE side produces nothing, the INSERT side does",
			"WITH moved AS (DELETE FROM staging.q RETURNING *) INSERT INTO archive.q SELECT * FROM moved",
			[]string{"archive.q"},
		},
		{
			"a qualified name that shares a CTE's name is a table",
			"WITH daily AS (SELECT 1) INSERT INTO analytics.daily SELECT * FROM daily",
			[]string{"analytics.daily"},
		},
		{
			"a nested WITH inside a CTE body",
			"WITH outer_cte AS (WITH inner_cte AS (SELECT 1) INSERT INTO a.t SELECT * FROM inner_cte RETURNING *) SELECT 1",
			[]string{"a.t"},
		},
	})
}

func TestExtractWriteTargets_EveryStatementIsRead(t *testing.T) {
	runWriteTargetCases(t, []writeTargetCase{
		{
			"three statements, three verbs",
			"INSERT INTO a.one SELECT 1; UPDATE a.two SET x = 1; MERGE INTO a.three USING s ON TRUE WHEN MATCHED THEN DELETE",
			[]string{"a.one", "a.two", "a.three"},
		},
		{
			"a leading semicolon and empty statements",
			";WITH c AS (SELECT 1) INSERT INTO dbo.t SELECT * FROM c;;",
			[]string{"dbo.t"},
		},
		{
			"a statement that writes nothing does not hide the next one",
			"SELECT * FROM a.src; DELETE FROM a.old; INSERT INTO a.dst SELECT * FROM a.src",
			[]string{"a.dst"},
		},
		{
			"a CTE's scope ends with its statement",
			"WITH t AS (SELECT 1) SELECT * FROM t; INSERT INTO t SELECT 1",
			[]string{"t"},
		},
		{
			"repeats collapse, case-insensitively, keeping the first spelling",
			"INSERT INTO a.t SELECT 1; insert into A.T select 2; UPDATE a.u SET x = 1",
			[]string{"a.t", "a.u"},
		},
		{
			"a semicolon inside parentheses does not split",
			"INSERT INTO a.t SELECT * FROM (SELECT 1; INSERT INTO a.fake SELECT 1) x",
			[]string{"a.t"},
		},
	})
}

func TestExtractWriteTargets_UpdateThroughAnAlias(t *testing.T) {
	runWriteTargetCases(t, []writeTargetCase{
		{
			"t-sql alias bound in FROM",
			"UPDATE o SET o.status = 'x' FROM dbo.orders o JOIN dbo.customers c ON c.id = o.customer_id",
			[]string{"dbo.orders"},
		},
		{
			"alias bound by a JOIN, with AS",
			"UPDATE c SET c.x = 1 FROM dbo.orders AS o INNER JOIN dbo.customers AS c ON c.id = o.cid",
			[]string{"dbo.customers"},
		},
		{
			"alias bound in a comma list",
			"UPDATE c SET x = 1 FROM dbo.orders o, dbo.customers c WHERE c.id = o.cid",
			[]string{"dbo.customers"},
		},
		{
			"an unaliased source exposes its own name",
			"UPDATE orders SET x = 1 FROM dbo.orders JOIN s ON s.id = orders.id",
			[]string{"dbo.orders"},
		},
		{
			"an aliased source does not expose its table name",
			"UPDATE orders SET x = 1 FROM dbo.orders AS o",
			[]string{"orders"},
		},
		{
			"postgres: FROM does not bind the target",
			"UPDATE orders SET total = s.total FROM staging s WHERE s.id = orders.id",
			[]string{"orders"},
		},
		{
			"IS DISTINCT FROM in SET is not the FROM clause",
			"UPDATE o SET flag = x IS DISTINCT FROM y FROM dbo.orders o",
			[]string{"dbo.orders"},
		},
		{
			"RETURNING ends the FROM list",
			"UPDATE t SET x = 1 FROM s RETURNING t.id, s.name AS t",
			[]string{"t"},
		},
		{
			"WHERE ends the FROM list",
			"UPDATE t SET x = 1 FROM s WHERE s.id = t.id RETURNING t.id, s.name AS t",
			[]string{"t"},
		},
		{
			"a subquery's FROM is not the statement's",
			"UPDATE o SET x = (SELECT max(v) FROM dbo.other o) FROM dbo.orders o",
			[]string{"dbo.orders"},
		},
		{
			// Only a bare name can be an alias. staging.orders exposing `orders` does not
			// make it the table analytics.orders names.
			"a schema-qualified target is its own table, whatever FROM exposes",
			"UPDATE analytics.orders SET total = 1 FROM staging.orders WHERE staging.orders.id = analytics.orders.id",
			[]string{"analytics.orders"},
		},
		{
			"the target's name as a column after a dot binds nothing",
			"UPDATE status SET label = c.label FROM staging.codes c JOIN staging.flags f ON f.status = c.status",
			[]string{"status"},
		},
		{
			"the target's name as a variable binds nothing",
			"UPDATE status SET label = c.label FROM staging.codes c JOIN staging.flags f ON f.id = c.id AND f.kind = @status",
			[]string{"status"},
		},
		{
			"a table inside a derived table binds nothing",
			"UPDATE orders SET total = s.total FROM (SELECT l.order_id, sum(l.amount) AS total " +
				"FROM staging.lines l JOIN staging.orders ON staging.orders.id = l.order_id GROUP BY l.order_id) s " +
				"WHERE s.order_id = orders.id",
			[]string{"orders"},
		},
		{
			"an aliased function call is known by its alias, not its function's name",
			"UPDATE totals SET amount = t.amount FROM dbo.totals(2026) AS t WHERE t.id = totals.id",
			[]string{"totals"},
		},
	})
}

func TestExtractWriteTargets_DollarQuotedTextIsAString(t *testing.T) {
	runWriteTargetCases(t, []writeTargetCase{
		{"a statement inside $$", "INSERT INTO a.t SELECT $$x; INSERT INTO analytics.fake SELECT 1$$", []string{"a.t"}},
		{"a tagged dollar quote", "INSERT INTO a.t SELECT $body$ ; UPDATE analytics.fake SET x = 1 $body$", []string{"a.t"}},
		{"a different tag does not close it", "INSERT INTO a.t SELECT $a$ $b$; INSERT INTO analytics.fake SELECT 1 $a$", []string{"a.t"}},
		{"an unterminated dollar quote", "INSERT INTO a.t SELECT $$; INSERT INTO analytics.fake SELECT 1", []string{"a.t"}},
		{"$$ inside a string opens nothing", "INSERT INTO a.t SELECT '$$'; INSERT INTO a.u SELECT '$$'", []string{"a.t", "a.u"}},
		{"$$ inside a quoted name opens nothing", `INSERT INTO "a$$" SELECT 1; INSERT INTO "b$$" SELECT 1`, []string{"a$$", "b$$"}},
		{"a positional parameter is not a quote", "INSERT INTO a.t VALUES ($1); INSERT INTO a.u VALUES ($2)", []string{"a.t", "a.u"}},
		{"$ inside a name is not a quote", "INSERT INTO a.t$$ VALUES (1); INSERT INTO a.u$$ VALUES (2)", []string{"a.t$$", "a.u$$"}},
		// A tag follows identifier rules, so it cannot start with a digit: `$1$` is a
		// parameter or a name, and the statement after it is still read.
		{"a tag cannot start with a digit", "INSERT INTO a.t SELECT $1$x FROM s; INSERT INTO a.u SELECT 1", []string{"a.t", "a.u"}},
		// Tool-written function bodies use upper case, underscores and digits in tags.
		// A tag not recognised would read the body's statements as the model's own.
		{
			"an upper-case tag",
			"INSERT INTO a.t SELECT $BODY$; INSERT INTO analytics.fake SELECT 1 $BODY$; INSERT INTO a.u SELECT 1",
			[]string{"a.t", "a.u"},
		},
		{
			"a tag with an underscore",
			"INSERT INTO a.t SELECT $fn_body$; INSERT INTO analytics.fake SELECT 1 $fn_body$; INSERT INTO a.u SELECT 1",
			[]string{"a.t", "a.u"},
		},
		{
			"a tag with a digit after its first character",
			"INSERT INTO a.t SELECT $v1$; INSERT INTO analytics.fake SELECT 1 $v1$; INSERT INTO a.u SELECT 1",
			[]string{"a.t", "a.u"},
		},
		// The closing tag ends the quote. Read again as an opening tag, it would swallow
		// every statement after it.
		{"the statement after a closed dollar quote is read", "INSERT INTO a.t SELECT $$x $$; INSERT INTO a.u SELECT 1", []string{"a.t", "a.u"}},
		{
			"the statement after a function body is read",
			"CREATE FUNCTION f() RETURNS void AS $fn$\nBEGIN\n  DELETE FROM a.old;\nEND;\n$fn$ LANGUAGE plpgsql;\nINSERT INTO a.u SELECT 1",
			[]string{"a.u"},
		},
		{
			"a quoted name that starts with $$ opens nothing",
			"INSERT INTO \"$$x\" SELECT 1; INSERT INTO `$$y` SELECT 1; INSERT INTO [$$z] SELECT 1; INSERT INTO a.u SELECT 1",
			[]string{"$$x", "$$y", "$$z", "a.u"},
		},
		// Comments go first: a `$$` inside one quotes nothing.
		{"a dollar quote inside a line comment opens nothing", "-- costs $$\nINSERT INTO a.t SELECT 1", []string{"a.t"}},
		{"a dollar quote inside a block comment opens nothing", "/* $body$ */ INSERT INTO a.t SELECT 1; INSERT INTO a.u SELECT 1", []string{"a.t", "a.u"}},
	})
}

// Every case names nothing; its control is the nearest SQL that does, which proves the
// case is empty for the reason its name gives.
func TestExtractWriteTargets_NamesNothingItCannotBeSureOf(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		control string
	}{
		{"SELECT only", "SELECT * FROM analytics.orders", "INSERT INTO analytics.orders SELECT 1"},
		{
			"SELECT with joins and subqueries",
			"SELECT * FROM a JOIN b ON TRUE WHERE id IN (SELECT id FROM c)",
			"INSERT INTO x SELECT * FROM a JOIN b ON TRUE WHERE id IN (SELECT id FROM c)",
		},
		{
			"INSERT inside a string literal",
			"SELECT 'x'; SELECT 'INSERT INTO analytics.orders VALUES (1)' AS note",
			"SELECT 'x'; INSERT INTO analytics.orders VALUES (1)",
		},
		{
			"a string literal holding a separator and a statement",
			"SELECT 'x; INSERT INTO analytics.orders SELECT 1'",
			"SELECT 'x'; INSERT INTO analytics.orders SELECT 1",
		},
		{
			"a line-commented statement",
			"-- INSERT INTO analytics.orders SELECT 1\nSELECT 1",
			"INSERT INTO analytics.orders SELECT 1\nSELECT 1",
		},
		{
			"a block-commented statement",
			"/* INSERT INTO analytics.orders SELECT 1; */ SELECT 1",
			"INSERT INTO analytics.orders SELECT 1; SELECT 1",
		},
		{
			"a comment after the only real statement",
			"SELECT 1; -- INSERT INTO analytics.orders SELECT 1",
			"SELECT 1; INSERT INTO analytics.orders SELECT 1",
		},
		{"DELETE removes rows", "DELETE FROM analytics.orders WHERE id = 1", "UPDATE analytics.orders SET id = 1"},
		{"TRUNCATE", "TRUNCATE TABLE analytics.orders", "CREATE TABLE analytics.orders AS SELECT 1"},
		{"DROP", "DROP TABLE analytics.orders", "CREATE TABLE analytics.orders AS SELECT 1"},
		{"ALTER", "ALTER TABLE analytics.orders ADD COLUMN x int", "CREATE TABLE analytics.orders AS SELECT 1"},
		{"EXPLAIN", "EXPLAIN INSERT INTO analytics.orders SELECT 1", "INSERT INTO analytics.orders SELECT 1"},
		{"CALL", "CALL load_orders()", "INSERT INTO analytics.orders SELECT 1"},
		{
			"CREATE TABLE with no query",
			"CREATE TABLE analytics.orders (id int PRIMARY KEY, total numeric)",
			"CREATE TABLE analytics.orders (id, total) AS SELECT 1, 2",
		},
		{
			"CREATE TABLE LIKE",
			"CREATE TABLE analytics.copy (LIKE analytics.orders INCLUDING ALL)",
			"CREATE TABLE analytics.copy AS SELECT * FROM analytics.orders",
		},
		{
			"a generated column's AS is inside the column list",
			"CREATE TABLE a.t (id int, x int GENERATED ALWAYS AS (id * 2) STORED)",
			"CREATE TABLE a.t AS (SELECT 1 AS id)",
		},
		{"t-sql AS NODE", "CREATE TABLE dbo.person (id int) AS NODE", "CREATE TABLE dbo.person AS SELECT 1 AS id"},
		{"t-sql edge table with no columns", "CREATE TABLE dbo.likes AS EDGE", "CREATE TABLE dbo.likes AS SELECT 1 AS id"},
		{
			// The query in parentheses only describes the columns; the table starts empty.
			"a CREATE TABLE whose query only supplies the columns",
			"CREATE TABLE analytics.t USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(LOCATION => '@stage/orders', FILE_FORMAT => 'fmt')))",
			"CREATE TABLE analytics.t AS SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM analytics.src",
		},
		{"postgres TEMP", "CREATE TEMP TABLE scratch AS SELECT 1", "CREATE TABLE scratch AS SELECT 1"},
		{"mysql TEMPORARY", "CREATE TEMPORARY TABLE scratch SELECT 1", "CREATE TABLE scratch SELECT 1"},
		{"GLOBAL TEMPORARY", "CREATE GLOBAL TEMPORARY TABLE scratch AS SELECT 1", "CREATE UNLOGGED TABLE scratch AS SELECT 1"},
		{"CREATE INDEX", "CREATE INDEX idx ON analytics.orders (id)", "CREATE VIEW idx AS SELECT 1"},
		{
			"CREATE PUBLICATION ... FOR TABLE",
			"CREATE PUBLICATION p FOR TABLE analytics.orders",
			"CREATE TABLE analytics.orders AS SELECT 1",
		},
		{
			"a function body is not a statement",
			"CREATE FUNCTION f() RETURNS TABLE (id int) AS $$ INSERT INTO analytics.orders VALUES (1) $$ LANGUAGE sql",
			"CREATE FUNCTION f() RETURNS int AS 'x'; INSERT INTO analytics.orders VALUES (1)",
		},
		{
			"a DO block",
			"DO $$ BEGIN INSERT INTO analytics.orders VALUES (1); END $$",
			"DO 'x'; INSERT INTO analytics.orders VALUES (1)",
		},
		{"t-sql temp table", "INSERT INTO #staging SELECT * FROM dbo.orders", "INSERT INTO staging SELECT * FROM dbo.orders"},
		{"t-sql global temp table", "INSERT INTO ##shared SELECT 1", "INSERT INTO shared SELECT 1"},
		{"a quoted temp table", "INSERT INTO [#staging] SELECT 1", "INSERT INTO [staging] SELECT 1"},
		{"a table variable", "INSERT INTO @rows SELECT 1", "INSERT INTO rows SELECT 1"},
		{
			"OPENQUERY",
			"INSERT INTO OPENQUERY(remote, 'SELECT id FROM t') SELECT 1",
			"INSERT INTO remote_orders (remote, id) SELECT 1",
		},
		{
			"OPENROWSET without INTO",
			"INSERT OPENROWSET('SQLNCLI', 'srv', 'SELECT 1') SELECT 1",
			"INSERT rowset (a) SELECT 1",
		},
		{"an empty argument list is a call", "INSERT INTO rowset() SELECT 1", "INSERT INTO rowset (a) SELECT 1"},
		{"a linked server", "INSERT INTO srv.db.dbo.orders SELECT 1", "INSERT INTO db.dbo.orders SELECT 1"},
		{"db..table leaves the schema unwritten", "INSERT INTO warehouse..orders SELECT 1", "INSERT INTO warehouse.dbo.orders SELECT 1"},
		{"a trailing dot", "INSERT INTO warehouse. SELECT 1", "INSERT INTO warehouse SELECT 1"},
		{"SELECT INTO", "SELECT * INTO analytics.copy FROM analytics.orders", "INSERT INTO analytics.copy SELECT * FROM analytics.orders"},
		{
			"Oracle INSERT ALL",
			"INSERT ALL INTO a.t1 VALUES (1) INTO a.t2 VALUES (2) SELECT * FROM dual",
			"INSERT INTO a.t1 VALUES (1)",
		},
		{"INSERT with the name missing", "INSERT INTO VALUES (1)", "INSERT INTO t VALUES (1)"},
		{"INSERT with the name missing before a row", "INSERT INTO VALUES ROW(1, 2)", "INSERT INTO analytics.t VALUES ROW(1, 2)"},
		{"INSERT with the name missing before DEFAULT", "INSERT INTO DEFAULT VALUES", "INSERT INTO analytics.t DEFAULT VALUES"},
		{
			"Oracle INSERT FIRST",
			"INSERT FIRST WHEN id > 1 THEN INTO a.t1 VALUES (1) SELECT * FROM dual",
			"INSERT INTO a.t1 VALUES (1)",
		},
		{"TOP after INTO", "INSERT INTO TOP 5 dbo.orders SELECT 1", "INSERT TOP (5) INTO dbo.orders SELECT 1"},
		{
			// These write files, not a table.
			"INSERT OVERWRITE DIRECTORY",
			"INSERT OVERWRITE DIRECTORY '/exports/orders' SELECT * FROM analytics.orders",
			"INSERT OVERWRITE TABLE analytics.orders_copy SELECT * FROM analytics.orders",
		},
		{
			"INSERT OVERWRITE LOCAL DIRECTORY",
			"INSERT OVERWRITE LOCAL DIRECTORY '/tmp/orders' SELECT * FROM analytics.orders",
			"INSERT OVERWRITE TABLE analytics.orders_copy SELECT * FROM analytics.orders",
		},
		{
			// Oracle: rows go into a collection column of whichever row the query finds.
			"INSERT into a nested table",
			"INSERT INTO TABLE(SELECT h.people FROM hr.info h WHERE h.id = 1) VALUES (1)",
			"INSERT INTO TABLE hr.people VALUES (1)",
		},
		{"TABLE after MERGE INTO", "MERGE INTO TABLE analytics.t USING s ON TRUE WHEN MATCHED THEN DELETE", "MERGE INTO analytics.t USING s ON TRUE WHEN MATCHED THEN DELETE"},
		{"IF without NOT EXISTS", "CREATE TABLE IF EXISTS analytics.t AS SELECT 1", "CREATE TABLE IF NOT EXISTS analytics.t AS SELECT 1"},
		{"OR REPLACE after TABLE", "CREATE TABLE OR REPLACE analytics.t AS SELECT 1", "CREATE OR REPLACE TABLE analytics.t AS SELECT 1"},
		{"VIEW written twice", "CREATE VIEW VIEW analytics.v AS SELECT 1", "CREATE VIEW analytics.v AS SELECT 1"},
		{
			"MERGE with the target missing",
			"MERGE INTO USING staging.c s ON TRUE WHEN MATCHED THEN DELETE",
			"MERGE INTO analytics.c USING staging.c s ON TRUE WHEN MATCHED THEN DELETE",
		},
		{"ONLY is never a name", "INSERT INTO ONLY analytics.t SELECT 1", "INSERT INTO analytics.t SELECT 1"},
		{"an unclosed column list", "INSERT INTO analytics.t (id, total", "INSERT INTO analytics.t (id, total) SELECT 1, 2"},
		{"INSERT into a subquery", "INSERT INTO (SELECT 1) VALUES (1)", "INSERT INTO t (a) VALUES (1)"},
		{
			"mysql multi-table UPDATE with JOIN",
			"UPDATE shop.orders o JOIN shop.customers c ON c.id = o.customer_id SET o.name = c.name",
			"UPDATE shop.orders o SET o.name = 'x'",
		},
		{
			"mysql multi-table UPDATE with a comma",
			"UPDATE shop.orders, shop.customers SET orders.x = customers.x",
			"UPDATE shop.orders SET orders.x = 1",
		},
		{
			"an UPDATE alias bound to a derived table",
			"UPDATE x SET a = 1 FROM (SELECT * FROM dbo.orders) x",
			"UPDATE x SET a = 1 FROM dbo.orders x",
		},
		{
			"an UPDATE alias bound to a function call",
			"UPDATE x SET a = 1 FROM dbo.fn_rows(1) x",
			"UPDATE x SET a = 1 FROM dbo.rows x",
		},
		{
			"an UPDATE target bound to a function call with no alias",
			"UPDATE totals SET amount = 1 FROM dbo.totals(2026)",
			"UPDATE totals SET amount = 1 FROM dbo.totals(2026) AS t",
		},
		{
			"an UPDATE alias bound by CROSS APPLY",
			"UPDATE x SET a = 1 FROM dbo.orders o CROSS APPLY (SELECT o.id AS a) x",
			"UPDATE o SET a = 1 FROM dbo.orders o CROSS APPLY (SELECT o.id AS a) x",
		},
		{
			"an UPDATE alias bound by CROSS APPLY, written in another case",
			"UPDATE X SET a = 1 FROM dbo.orders o CROSS APPLY (SELECT o.id AS a) x",
			"UPDATE O SET a = 1 FROM dbo.orders o CROSS APPLY (SELECT o.id AS a) x",
		},
		{
			"an UPDATE alias bound by OUTER APPLY to a function",
			"UPDATE f SET a = 1 FROM dbo.orders o OUTER APPLY dbo.fn_lines(o.id) f",
			"UPDATE o SET a = 1 FROM dbo.orders o OUTER APPLY dbo.fn_lines(o.id) f",
		},
		{
			"an UPDATE alias bound by PIVOT",
			"UPDATE p SET a = 1 FROM dbo.sales PIVOT (SUM(amount) FOR yr IN ([2025], [2026])) AS p",
			"UPDATE s SET a = 1 FROM dbo.sales AS s PIVOT (SUM(amount) FOR yr IN ([2025], [2026])) AS p",
		},
		{
			"an UPDATE alias bound to a table variable",
			"UPDATE x SET a = 1 FROM @rows x",
			"UPDATE x SET a = 1 FROM dbo.rows x",
		},
		{
			"a CTE name is never a target",
			"WITH daily AS (SELECT 1) INSERT INTO daily SELECT * FROM daily",
			"WITH other AS (SELECT 1) INSERT INTO daily SELECT * FROM other",
		},
		// A CTE name matches without regard to case, as the engines match it.
		{
			"a CTE named in mixed case, used in the same case",
			"WITH Latest AS (SELECT * FROM dbo.orders) UPDATE Latest SET x = 1",
			"WITH Latest AS (SELECT * FROM dbo.orders) UPDATE dbo.orders SET x = 1",
		},
		{
			"a CTE named in lower case, used in upper case",
			"WITH latest AS (SELECT * FROM dbo.orders) UPDATE LATEST SET x = 1",
			"WITH latest AS (SELECT * FROM dbo.orders) UPDATE orders SET x = 1",
		},
		{
			// An apostrophe in a comment must not open a string that hides the $$ after it.
			"a quote inside a comment does not end dollar quoting",
			"-- don't\nSELECT $$x; INSERT INTO analytics.fake SELECT 1$$",
			"-- don't\nSELECT 'x'; INSERT INTO analytics.fake SELECT 1",
		},
		{
			"a WITH list without a body",
			"WITH a AS SELECT 1 INSERT INTO t SELECT 1",
			"WITH a AS (SELECT 1) INSERT INTO t SELECT 1",
		},
		{
			"an unclosed WITH body",
			"WITH a AS (SELECT 1 INSERT INTO t SELECT 1",
			"WITH a AS (SELECT 1) INSERT INTO t SELECT 1",
		},
		{
			"a CTE name with no body before the verb",
			"WITH a INSERT INTO analytics.t SELECT 1",
			"WITH a AS (SELECT 1) INSERT INTO analytics.t SELECT 1",
		},
		{
			"a writing CTE in a list that breaks off",
			"WITH a AS (INSERT INTO analytics.x SELECT 1 RETURNING id), b AS SELECT * FROM a",
			"WITH a AS (INSERT INTO analytics.x SELECT 1 RETURNING id) SELECT * FROM a",
		},
		{
			"a writing CTE followed by a body with no name",
			"WITH a AS (INSERT INTO analytics.x SELECT 1 RETURNING id), (SELECT 1) SELECT * FROM a",
			"WITH a AS (INSERT INTO analytics.x SELECT 1 RETURNING id) SELECT * FROM a",
		},
		{"WITH and nothing after", "WITH", "WITH a AS (SELECT 1) INSERT INTO t SELECT 1"},
		{"a CTE-led SELECT", "WITH a AS (SELECT 1) SELECT * FROM a", "WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a"},
		{"a quoted verb is not a verb", `"INSERT" INTO a.t SELECT 1`, "INSERT INTO a.t SELECT 1"},
		{"REFRESH without MATERIALIZED VIEW", "REFRESH analytics.mv", "REFRESH MATERIALIZED VIEW analytics.mv"},
		{"REFRESH MATERIALIZED without VIEW", "REFRESH MATERIALIZED analytics.mv", "REFRESH MATERIALIZED VIEW analytics.mv"},
		{"CREATE with an unknown prefix", "CREATE SECURE VIEW analytics.v AS SELECT 1", "CREATE OR REPLACE VIEW analytics.v AS SELECT 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := writeTargetNames(tc.sql); len(got) != 0 {
				t.Fatalf("ExtractWriteTargets(%q) = %q, want nothing", tc.sql, got)
			}
			if got := writeTargetNames(tc.control); len(got) == 0 {
				t.Fatalf("control %q named nothing, so the case above proves nothing", tc.control)
			}
		})
	}
}

func TestExtractWriteTargets_EmptyInputCostsNothing(t *testing.T) {
	for _, sql := range []string{"", "   ", "-- only a comment", "/* nothing */", ";", "$$", "'"} {
		if got := ExtractWriteTargets(sql); len(got) != 0 {
			t.Fatalf("ExtractWriteTargets(%q) = %v, want nothing", sql, got)
		}
	}
}

// Every prefix of these statements is malformed SQL of some kind. None may panic, and
// none may hang: the walk only moves forward.
func TestExtractWriteTargets_TruncatedSQLNeverPanics(t *testing.T) {
	statements := []string{
		`WITH RECURSIVE a (n) AS NOT MATERIALIZED (SELECT 1), b AS (INSERT INTO x.y (c) VALUES ($1) RETURNING *) INSERT INTO "A"."B" (c) SELECT * FROM b;`,
		"UPDATE TOP (5) o WITH (ROWLOCK) AS o SET o.x = y IS DISTINCT FROM z FROM dbo.orders AS o JOIN (SELECT 1) d ON TRUE, dbo.fn(1) f WHERE 1 = 1 RETURNING *;",
		"CREATE OR REPLACE UNLOGGED TABLE IF NOT EXISTS [a].[b] (c, d) AS (SELECT $tag$ ; $tag$);",
		"MERGE TOP (1) PERCENT INTO t USING s ON TRUE; REFRESH MATERIALIZED VIEW CONCURRENTLY mv; INSERT LOW_PRIORITY IGNORE INTO `q` SELECT '$$';",
	}
	calls := 0
	for _, sql := range statements {
		for i := 0; i <= len(sql); i++ {
			_ = ExtractWriteTargets(sql[:i])
			calls++
		}
	}
	if calls == 0 {
		t.Fatal("no prefixes were exercised")
	}

	long := "INSERT INTO base SELECT 1" + strings.Repeat("; INSERT INTO t SELECT 1", 2000)
	if got := writeTargetNames(long); len(got) != 2 {
		t.Fatalf("want base and t, got %d targets", len(got))
	}
	deep := "INSERT INTO deep SELECT " + strings.Repeat("(", 500) + "1" + strings.Repeat(")", 500)
	assertWriteTargets(t, deep, "deep")
}

// IsSingleStatement is the run check's single-statement rule, asked ahead of time. Each
// case is also put to ValidateExplorerStatement as the owner, who may run any write, so
// the two cannot drift apart: where it says one statement, the run check does not refuse
// the SQL as several statements or as empty.
func TestIsSingleStatement(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"INSERT INTO a.t SELECT 1", true},
		{"INSERT INTO a.t SELECT 1;", true},
		{"  INSERT INTO a.t SELECT 1 ;\n", true},
		{"INSERT INTO a.t SELECT 'a;b'", true},
		{`INSERT INTO "a;b" SELECT 1`, true},
		{"INSERT INTO a.t SELECT 1 -- nightly; keep", true},
		{"/* step 1; step 2 */ INSERT INTO a.t SELECT 1", true},
		{"INSERT INTO a.t SELECT 1; -- done", true},

		{"INSERT INTO a.t SELECT 1; INSERT INTO a.u SELECT 1", false},
		{"INSERT INTO a.t SELECT 1;;", false},
		{"INSERT INTO a.t SELECT 1; -- done\nUPDATE a.u SET x = 1", false},
		// The run check knows nothing of dollar quotes or backslash escapes, so it
		// refuses both, and a model built on either never runs.
		{"INSERT INTO a.t SELECT $$a;b$$", false},
		{`INSERT INTO a.t SELECT 'it\'s; fine'`, false},
		{"", false},
		{"   ", false},
		{"-- only a comment", false},
		{"/* nothing */", false},
	}
	var single, several int
	for _, tc := range cases {
		got := IsSingleStatement(tc.sql)
		if got != tc.want {
			t.Errorf("IsSingleStatement(%q) = %v, want %v", tc.sql, got, tc.want)
		}
		v := ValidateExplorerStatement(tc.sql, security.WSOwner)
		refused := v != nil && !v.Valid &&
			(v.ErrorCode == ErrCodeMultipleStatements || v.ErrorCode == ErrCodeEmptyQuery)
		if got == refused {
			t.Errorf("IsSingleStatement(%q) = %v, but the run check refused it as several or none: %v", tc.sql, got, refused)
		}
		if tc.want {
			single++
		} else {
			several++
		}
	}
	if single == 0 || several == 0 {
		t.Fatalf("cases must cover both answers: %d single, %d not", single, several)
	}
}
