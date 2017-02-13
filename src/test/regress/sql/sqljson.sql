-- JSON_OBJECT()
SELECT JSON_OBJECT();
SELECT JSON_OBJECT(RETURNING json);
SELECT JSON_OBJECT(RETURNING json FORMAT JSON);
SELECT JSON_OBJECT(RETURNING jsonb);
SELECT JSON_OBJECT(RETURNING jsonb FORMAT JSON);
SELECT JSON_OBJECT(RETURNING text);
SELECT JSON_OBJECT(RETURNING text FORMAT JSON);
SELECT JSON_OBJECT(RETURNING text FORMAT JSON ENCODING UTF8);
SELECT JSON_OBJECT(RETURNING text FORMAT JSON ENCODING INVALID_ENCODING);
SELECT JSON_OBJECT(RETURNING bytea);
SELECT JSON_OBJECT(RETURNING bytea FORMAT JSON);
SELECT JSON_OBJECT(RETURNING bytea FORMAT JSON ENCODING UTF8);
SELECT JSON_OBJECT(RETURNING bytea FORMAT JSON ENCODING UTF16);
SELECT JSON_OBJECT(RETURNING bytea FORMAT JSON ENCODING UTF32);

SELECT JSON_OBJECT('foo': NULL::int FORMAT JSON);
SELECT JSON_OBJECT('foo': NULL::int FORMAT JSON ENCODING UTF8);
SELECT JSON_OBJECT('foo': NULL::json FORMAT JSON);
SELECT JSON_OBJECT('foo': NULL::json FORMAT JSON ENCODING UTF8);
SELECT JSON_OBJECT('foo': NULL::jsonb FORMAT JSON);
SELECT JSON_OBJECT('foo': NULL::jsonb FORMAT JSON ENCODING UTF8);

SELECT JSON_OBJECT(NULL: 1);
SELECT JSON_OBJECT('a': 2 + 3);
SELECT JSON_OBJECT('a' VALUE 2 + 3);
--SELECT JSON_OBJECT(KEY 'a' VALUE 2 + 3);
SELECT JSON_OBJECT('a' || 2: 1);
SELECT JSON_OBJECT(('a' || 2) VALUE 1);
--SELECT JSON_OBJECT('a' || 2 VALUE 1);
--SELECT JSON_OBJECT(KEY 'a' || 2 VALUE 1);
SELECT JSON_OBJECT('a': 2::text);
SELECT JSON_OBJECT('a' VALUE 2::text);
--SELECT JSON_OBJECT(KEY 'a' VALUE 2::text);
SELECT JSON_OBJECT(1::text: 2);
SELECT JSON_OBJECT((1::text) VALUE 2);
--SELECT JSON_OBJECT(1::text VALUE 2);
--SELECT JSON_OBJECT(KEY 1::text VALUE 2);
SELECT JSON_OBJECT(json '[1]': 123);
SELECT JSON_OBJECT(ARRAY[1,2,3]: 'aaa');

SELECT JSON_OBJECT(
	'a': '123',
	1.23: 123,
	'c': json '[ 1,true,{ } ]',
	'd': jsonb '{ "x" : 123.45 }'
);

SELECT JSON_OBJECT(
	'a': '123',
	1.23: 123,
	'c': json '[ 1,true,{ } ]',
	'd': jsonb '{ "x" : 123.45 }'
	RETURNING jsonb
);

/*
SELECT JSON_OBJECT(
	'a': '123',
	KEY 1.23 VALUE 123,
	'c' VALUE json '[1, true, {}]'
);
*/

SELECT JSON_OBJECT('a': '123', 'b': JSON_OBJECT('a': 111, 'b': 'aaa'));
SELECT JSON_OBJECT('a': '123', 'b': JSON_OBJECT('a': 111, 'b': 'aaa' RETURNING jsonb));

SELECT JSON_OBJECT('a': JSON_OBJECT('b': 1 RETURNING text));
SELECT JSON_OBJECT('a': JSON_OBJECT('b': 1 RETURNING text) FORMAT JSON);
SELECT JSON_OBJECT('a': JSON_OBJECT('b': 1 RETURNING bytea));
SELECT JSON_OBJECT('a': JSON_OBJECT('b': 1 RETURNING bytea) FORMAT JSON);

SELECT JSON_OBJECT('a': '1', 'b': NULL, 'c': 2);
SELECT JSON_OBJECT('a': '1', 'b': NULL, 'c': 2 NULL ON NULL);
SELECT JSON_OBJECT('a': '1', 'b': NULL, 'c': 2 ABSENT ON NULL);

SELECT JSON_OBJECT(1: 1, '1': NULL WITH UNIQUE);
SELECT JSON_OBJECT(1: 1, '1': NULL ABSENT ON NULL WITH UNIQUE);
SELECT JSON_OBJECT(1: 1, '1': NULL NULL ON NULL WITH UNIQUE RETURNING jsonb);
SELECT JSON_OBJECT(1: 1, '1': NULL ABSENT ON NULL WITH UNIQUE RETURNING jsonb);

SELECT JSON_OBJECT(1: 1, '2': NULL, '1': 1 NULL ON NULL WITH UNIQUE);
SELECT JSON_OBJECT(1: 1, '2': NULL, '1': 1 ABSENT ON NULL WITH UNIQUE);
SELECT JSON_OBJECT(1: 1, '2': NULL, '1': 1 ABSENT ON NULL WITHOUT UNIQUE);
SELECT JSON_OBJECT(1: 1, '2': NULL, '1': 1 ABSENT ON NULL WITH UNIQUE RETURNING jsonb);
SELECT JSON_OBJECT(1: 1, '2': NULL, '1': 1 ABSENT ON NULL WITHOUT UNIQUE RETURNING jsonb);
SELECT JSON_OBJECT(1: 1, '2': NULL, '3': 1, 4: NULL, '5': 'a' ABSENT ON NULL WITH UNIQUE RETURNING jsonb);


-- JSON_ARRAY()
SELECT JSON_ARRAY();
SELECT JSON_ARRAY(RETURNING json);
SELECT JSON_ARRAY(RETURNING json FORMAT JSON);
SELECT JSON_ARRAY(RETURNING jsonb);
SELECT JSON_ARRAY(RETURNING jsonb FORMAT JSON);
SELECT JSON_ARRAY(RETURNING text);
SELECT JSON_ARRAY(RETURNING text FORMAT JSON);
SELECT JSON_ARRAY(RETURNING text FORMAT JSON ENCODING UTF8);
SELECT JSON_ARRAY(RETURNING text FORMAT JSON ENCODING INVALID_ENCODING);
SELECT JSON_ARRAY(RETURNING bytea);
SELECT JSON_ARRAY(RETURNING bytea FORMAT JSON);
SELECT JSON_ARRAY(RETURNING bytea FORMAT JSON ENCODING UTF8);
SELECT JSON_ARRAY(RETURNING bytea FORMAT JSON ENCODING UTF16);
SELECT JSON_ARRAY(RETURNING bytea FORMAT JSON ENCODING UTF32);

SELECT JSON_ARRAY('aaa', 111, true, array[1,2,3], NULL, json '{"a": [1]}', jsonb '["a",3]');

SELECT JSON_ARRAY('a',  NULL, 'b' NULL   ON NULL);
SELECT JSON_ARRAY('a',  NULL, 'b' ABSENT ON NULL);
SELECT JSON_ARRAY(NULL, NULL, 'b' ABSENT ON NULL);
SELECT JSON_ARRAY('a',  NULL, 'b' NULL   ON NULL RETURNING jsonb);
SELECT JSON_ARRAY('a',  NULL, 'b' ABSENT ON NULL RETURNING jsonb);
SELECT JSON_ARRAY(NULL, NULL, 'b' ABSENT ON NULL RETURNING jsonb);

SELECT JSON_ARRAY(JSON_ARRAY('{ "a" : 123 }' RETURNING text));
SELECT JSON_ARRAY(JSON_ARRAY('{ "a" : 123 }' FORMAT JSON RETURNING text));
SELECT JSON_ARRAY(JSON_ARRAY('{ "a" : 123 }' FORMAT JSON RETURNING text) FORMAT JSON);

-- JSON_ARRAYAGG()
SELECT	JSON_ARRAYAGG(i) IS NULL,
		JSON_ARRAYAGG(i RETURNING jsonb) IS NULL
FROM generate_series(1, 0) i;

SELECT	JSON_ARRAYAGG(i),
		JSON_ARRAYAGG(i RETURNING jsonb)
FROM generate_series(1, 5) i;

SELECT JSON_ARRAYAGG(i ORDER BY i DESC)
FROM generate_series(1, 5) i;

SELECT JSON_ARRAYAGG(i::text::json)
FROM generate_series(1, 5) i;

SELECT JSON_ARRAYAGG(JSON_ARRAY(i, i + 1 RETURNING text) FORMAT JSON)
FROM generate_series(1, 5) i;

SELECT	JSON_ARRAYAGG(NULL),
		JSON_ARRAYAGG(NULL RETURNING jsonb)
FROM generate_series(1, 5);

SELECT	JSON_ARRAYAGG(NULL NULL ON NULL),
		JSON_ARRAYAGG(NULL NULL ON NULL RETURNING jsonb)
FROM generate_series(1, 5);

SELECT
	JSON_ARRAYAGG(bar),
	JSON_ARRAYAGG(bar RETURNING jsonb),
	JSON_ARRAYAGG(bar ABSENT ON NULL),
	JSON_ARRAYAGG(bar ABSENT ON NULL RETURNING jsonb),
	JSON_ARRAYAGG(bar NULL ON NULL),
	JSON_ARRAYAGG(bar NULL ON NULL RETURNING jsonb),
	JSON_ARRAYAGG(foo),
	JSON_ARRAYAGG(foo RETURNING jsonb),
	JSON_ARRAYAGG(foo ORDER BY bar) FILTER (WHERE bar > 2),
	JSON_ARRAYAGG(foo ORDER BY bar RETURNING jsonb) FILTER (WHERE bar > 2)
FROM
	(VALUES (NULL), (3), (1), (NULL), (NULL), (5), (2), (4), (NULL)) foo(bar);

SELECT
	bar, JSON_ARRAYAGG(bar) FILTER (WHERE bar > 2) OVER (PARTITION BY foo.bar % 2)
FROM
	(VALUES (NULL), (3), (1), (NULL), (NULL), (5), (2), (4), (NULL), (5), (4)) foo(bar);

-- JSON_OBJECTAGG()
SELECT	JSON_OBJECTAGG('key': 1) IS NULL,
		JSON_OBJECTAGG('key': 1 RETURNING jsonb) IS NULL
WHERE FALSE;

SELECT JSON_OBJECTAGG(NULL: 1);

SELECT JSON_OBJECTAGG(NULL: 1 RETURNING jsonb);

SELECT
	JSON_OBJECTAGG(i: i),
--	JSON_OBJECTAGG(i VALUE i),
--	JSON_OBJECTAGG(KEY i VALUE i),
	JSON_OBJECTAGG(i: i RETURNING jsonb)
FROM
	generate_series(1, 5) i;

SELECT
	JSON_OBJECTAGG(k: v),
	JSON_OBJECTAGG(k: v NULL ON NULL),
	JSON_OBJECTAGG(k: v ABSENT ON NULL),
	JSON_OBJECTAGG(k: v RETURNING jsonb),
	JSON_OBJECTAGG(k: v NULL ON NULL RETURNING jsonb),
	JSON_OBJECTAGG(k: v ABSENT ON NULL RETURNING jsonb)
FROM
	(VALUES (1, 1), (1, NULL), (2, NULL), (3, 3)) foo(k, v);

SELECT JSON_OBJECTAGG(k: v WITH UNIQUE KEYS)
FROM (VALUES (1, 1), (1, NULL), (2, 2)) foo(k, v);

SELECT JSON_OBJECTAGG(k: v ABSENT ON NULL WITH UNIQUE KEYS)
FROM (VALUES (1, 1), (1, NULL), (2, 2)) foo(k, v);

SELECT JSON_OBJECTAGG(k: v ABSENT ON NULL WITH UNIQUE KEYS)
FROM (VALUES (1, 1), (0, NULL), (3, NULL), (2, 2), (4, NULL)) foo(k, v);

SELECT JSON_OBJECTAGG(k: v WITH UNIQUE KEYS RETURNING jsonb)
FROM (VALUES (1, 1), (1, NULL), (2, 2)) foo(k, v);

SELECT JSON_OBJECTAGG(k: v ABSENT ON NULL WITH UNIQUE KEYS RETURNING jsonb)
FROM (VALUES (1, 1), (1, NULL), (2, 2)) foo(k, v);
