select jsonb '{"a": 12}' @? '$.a.b';
select jsonb '{"a": 12}' @? '$.b';
select jsonb '{"a": {"a": 12}}' @? '$.a.a';
select jsonb '{"a": {"a": 12}}' @? '$.*.a';
select jsonb '{"b": {"a": 12}}' @? '$.*.a';
select jsonb '{}' @? '$.*';
select jsonb '{"a": 1}' @? '$.*';
select jsonb '{"a": {"b": 1}}' @? 'lax $.**{1}';
select jsonb '{"a": {"b": 1}}' @? 'lax $.**{2}';
select jsonb '{"a": {"b": 1}}' @? 'lax $.**{3}';
select jsonb '[]' @? '$[*]';
select jsonb '[1]' @? '$[*]';
select jsonb '[1]' @? '$[1]';
select jsonb '[1]' @? 'strict $[1]';
select jsonb '[1]' @? '$[0]';
select jsonb '[1]' @? '$[0.3]';
select jsonb '[1]' @? '$[0.5]';
select jsonb '[1]' @? '$[0.9]';
select jsonb '[1]' @? '$[1.2]';
select jsonb '[1]' @? 'strict $[1.2]';
select jsonb '{}' @? 'strict $[0.3]';
select jsonb '{}' @? 'lax $[0.3]';
select jsonb '{}' @? 'strict $[1.2]';
select jsonb '{}' @? 'lax $[1.2]';
select jsonb '{}' @? 'strict $[-2 to 3]';
select jsonb '{}' @? 'lax $[-2 to 3]';
select jsonb '{"a": [1,2,3], "b": [3,4,5]}' @? '$ ? (@.a[*] >  @.b[*])';
select jsonb '{"a": [1,2,3], "b": [3,4,5]}' @? '$ ? (@.a[*] >= @.b[*])';
select jsonb '{"a": [1,2,3], "b": [3,4,"5"]}' @? '$ ? (@.a[*] >= @.b[*])';
select jsonb '{"a": [1,2,3], "b": [3,4,"5"]}' @? 'strict $ ? (@.a[*] >= @.b[*])';
select jsonb '{"a": [1,2,3], "b": [3,4,null]}' @? '$ ? (@.a[*] >= @.b[*])';
select jsonb '1' @? '$ ? ((@ == "1") is unknown)';
select jsonb '1' @? '$ ? ((@ == 1) is unknown)';
select jsonb '[{"a": 1}, {"a": 2}]' @? '$[0 to 1] ? (@.a > 1)';

select jsonb '{"a": 12, "b": {"a": 13}}' @* '$.a';
select jsonb '{"a": 12, "b": {"a": 13}}' @* '$.b';
select jsonb '{"a": 12, "b": {"a": 13}}' @* '$.*';
select jsonb '{"a": 12, "b": {"a": 13}}' @* 'lax $.*.a';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[*].a';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[*].*';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[0].a';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[1].a';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[2].a';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[0,1].a';
select jsonb '[12, {"a": 13}, {"b": 14}]' @* 'lax $[0 to 10].a';
select jsonb '[12, {"a": 13}, {"b": 14}, "ccc", true]' @* '$[2.5 - 1 to @.size() - 2]';
select jsonb '1' @* 'lax $[0]';
select jsonb '1' @* 'lax $[*]';
select jsonb '{}' @* 'lax $[0]';
select jsonb '[1]' @* 'lax $[0]';
select jsonb '[1]' @* 'lax $[*]';
select jsonb '[1,2,3]' @* 'lax $[*]';
select jsonb '[]' @* '$[last]';
select jsonb '[]' @* 'strict $[last]';
select jsonb '[1]' @* '$[last]';
select jsonb '{}' @* 'lax $[last]';
select jsonb '[1,2,3]' @* '$[last]';
select jsonb '[1,2,3]' @* '$[last - 1]';
select jsonb '[1,2,3]' @* '$[last ? (@.type() == "number")]';
select jsonb '[1,2,3]' @* '$[last ? (@.type() == "string")]';

select * from jsonpath_query(jsonb '{"a": 10}', '$');
select * from jsonpath_query(jsonb '{"a": 10}', '$ ? (.a < $value)');
select * from jsonpath_query(jsonb '{"a": 10}', '$ ? (.a < $value)', '{"value" : 13}');
select * from jsonpath_query(jsonb '{"a": 10}', '$ ? (.a < $value)', '{"value" : 8}');
select * from jsonpath_query(jsonb '{"a": 10}', '$.a ? (@ < $value)', '{"value" : 13}');
select * from jsonpath_query(jsonb '[10,11,12,13,14,15]', '$[*] ? (@ < $value)', '{"value" : 13}');
select * from jsonpath_query(jsonb '[10,11,12,13,14,15]', '$[0,1] ? (@ < $value)', '{"value" : 13}');
select * from jsonpath_query(jsonb '[10,11,12,13,14,15]', '$[0 to 2] ? (@ < $value)', '{"value" : 15}');
select * from jsonpath_query(jsonb '[1,"1",2,"2",null]', '$[*] ? (@ == "1")');
select * from jsonpath_query(jsonb '[1,"1",2,"2",null]', '$[*] ? (@ == $value)', '{"value" : "1"}');
select * from jsonpath_query(jsonb '[1, "2", null]', '$[*] ? (@ != null)');
select * from jsonpath_query(jsonb '[1, "2", null]', '$[*] ? (@ == null)');

select jsonb '{"a": {"b": 1}}' @* 'lax $.**';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{1}';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{1,}';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{2}';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{2,}';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{3,}';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**.b ? (@ > 0)';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{0}.b ? (@ > 0)';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{1}.b ? (@ > 0)';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{0,}.b ? (@ > 0)';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{1,}.b ? (@ > 0)';
select jsonb '{"a": {"b": 1}}' @* 'lax $.**{1,2}.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**{0}.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**{1}.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**{0,}.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**{1,}.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**{1,2}.b ? (@ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @* 'lax $.**{2,3}.b ? (@ > 0)';

select jsonb '{"a": {"b": 1}}' @? '$.**.b ? ( @ > 0)';
select jsonb '{"a": {"b": 1}}' @? '$.**{0}.b ? ( @ > 0)';
select jsonb '{"a": {"b": 1}}' @? '$.**{1}.b ? ( @ > 0)';
select jsonb '{"a": {"b": 1}}' @? '$.**{0,}.b ? ( @ > 0)';
select jsonb '{"a": {"b": 1}}' @? '$.**{1,}.b ? ( @ > 0)';
select jsonb '{"a": {"b": 1}}' @? '$.**{1,2}.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**{0}.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**{1}.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**{0,}.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**{1,}.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**{1,2}.b ? ( @ > 0)';
select jsonb '{"a": {"c": {"b": 1}}}' @? '$.**{2,3}.b ? ( @ > 0)';

select jsonb '{"g": {"x": 2}}' @* '$.g ? (exists (@.x))';
select jsonb '{"g": {"x": 2}}' @* '$.g ? (exists (@.y))';
select jsonb '{"g": {"x": 2}}' @* '$.g ? (exists (@.x ? (@ >= 2) ))';

--test ternary logic
select
	x, y,
	jsonpath_query(
		jsonb '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true && $y == true) ||
				 @ == false && !($x == true && $y == true) ||
				 @ == null  &&  ($x == true && $y == true) is unknown)',
		jsonb_build_object('x', x, 'y', y)
	) as "x && y"
from
	(values (jsonb 'true'), ('false'), ('"null"')) x(x),
	(values (jsonb 'true'), ('false'), ('"null"')) y(y);

select
	x, y,
	jsonpath_query(
		jsonb '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true || $y == true) ||
				 @ == false && !($x == true || $y == true) ||
				 @ == null  &&  ($x == true || $y == true) is unknown)',
		jsonb_build_object('x', x, 'y', y)
	) as "x || y"
from
	(values (jsonb 'true'), ('false'), ('"null"')) x(x),
	(values (jsonb 'true'), ('false'), ('"null"')) y(y);

select jsonb '{"a": 1, "b":1}' @? '$ ? (.a == .b)';
select jsonb '{"c": {"a": 1, "b":1}}' @? '$ ? (.a == .b)';
select jsonb '{"c": {"a": 1, "b":1}}' @? '$.c ? (.a == .b)';
select jsonb '{"c": {"a": 1, "b":1}}' @? '$.c ? ($.c.a == .b)';
select jsonb '{"c": {"a": 1, "b":1}}' @? '$.* ? (.a == .b)';
select jsonb '{"a": 1, "b":1}' @? '$.** ? (.a == .b)';
select jsonb '{"c": {"a": 1, "b":1}}' @? '$.** ? (.a == .b)';

select jsonb '{"c": {"a": 2, "b":1}}' @* '$.** ? (.a == 1 + 1)';
select jsonb '{"c": {"a": 2, "b":1}}' @* '$.** ? (.a == (1 + 1))';
select jsonb '{"c": {"a": 2, "b":1}}' @* '$.** ? (.a == .b + 1)';
select jsonb '{"c": {"a": 2, "b":1}}' @* '$.** ? (.a == (.b + 1))';
select jsonb '{"c": {"a": -1, "b":1}}' @? '$.** ? (.a == - 1)';
select jsonb '{"c": {"a": -1, "b":1}}' @? '$.** ? (.a == -1)';
select jsonb '{"c": {"a": -1, "b":1}}' @? '$.** ? (.a == -.b)';
select jsonb '{"c": {"a": -1, "b":1}}' @? '$.** ? (.a == - .b)';
select jsonb '{"c": {"a": 0, "b":1}}' @? '$.** ? (.a == 1 - .b)';
select jsonb '{"c": {"a": 2, "b":1}}' @? '$.** ? (.a == 1 - - .b)';
select jsonb '{"c": {"a": 0, "b":1}}' @? '$.** ? (.a == 1 - +.b)';
select jsonb '[1,2,3]' @? '$ ? (+@[*] > +2)';
select jsonb '[1,2,3]' @? '$ ? (+@[*] > +3)';
select jsonb '[1,2,3]' @? '$ ? (-@[*] < -2)';
select jsonb '[1,2,3]' @? '$ ? (-@[*] < -3)';
select jsonb '1' @? '$ ? ($ > 0)';

-- unwrapping of operator arguments in lax mode
select jsonb '{"a": [2]}' @* 'lax $.a * 3';
select jsonb '{"a": [2]}' @* 'lax $.a + 3';
select jsonb '{"a": [2, 3, 4]}' @* 'lax -$.a';
-- should fail
select jsonb '{"a": [1, 2]}' @* 'lax $.a * 3';

-- extension: boolean expressions
select jsonb '2' @* '$ > 1';
select jsonb '2' @* '$ <= 1';
select jsonb '2' @* '$ == "2"';

select jsonb '2' @~ '$ > 1';
select jsonb '2' @~ '$ <= 1';
select jsonb '2' @~ '$ == "2"';
select jsonb '2' @~ '1';
select jsonb '{}' @~ '$';
select jsonb '[]' @~ '$';
select jsonb '[1,2,3]' @~ '$[*]';
select jsonb '[]' @~ '$[*]';
select jsonpath_predicate(jsonb '[[1, true], [2, false]]', 'strict $[*] ? (@[0] > $x) [1]', '{"x": 1}');
select jsonpath_predicate(jsonb '[[1, true], [2, false]]', 'strict $[*] ? (@[0] < $x) [1]', '{"x": 2}');

select jsonb '[null,1,true,"a",[],{}]' @* '$.type()';
select jsonb '[null,1,true,"a",[],{}]' @* 'lax $.type()';
select jsonb '[null,1,true,"a",[],{}]' @* '$[*].type()';
select jsonb 'null' @* 'null.type()';
select jsonb 'null' @* 'true.type()';
select jsonb 'null' @* '123.type()';
select jsonb 'null' @* '"123".type()';

select jsonb '{"a": 2}' @* '($.a - 5).abs() + 10';
select jsonb '{"a": 2.5}' @* '-($.a * $.a).floor() + 10';
select jsonb '[1, 2, 3]' @* '($[*] > 2) ? (@ == true)';
select jsonb '[1, 2, 3]' @* '($[*] > 3).type()';
select jsonb '[1, 2, 3]' @* '($[*].a > 3).type()';
select jsonb '[1, 2, 3]' @* 'strict ($[*].a > 3).type()';

select jsonb '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]' @* 'strict $[*].size()';
select jsonb '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]' @* 'lax $[*].size()';

select jsonb '[0, 1, -2, -3.4, 5.6]' @* '$[*].abs()';
select jsonb '[0, 1, -2, -3.4, 5.6]' @* '$[*].floor()';
select jsonb '[0, 1, -2, -3.4, 5.6]' @* '$[*].ceiling()';
select jsonb '[0, 1, -2, -3.4, 5.6]' @* '$[*].ceiling().abs()';
select jsonb '[0, 1, -2, -3.4, 5.6]' @* '$[*].ceiling().abs().type()';

select jsonb '[{},1]' @* '$[*].keyvalue()';
select jsonb '{}' @* '$.keyvalue()';
select jsonb '{"a": 1, "b": [1, 2], "c": {"a": "bbb"}}' @* '$.keyvalue()';
select jsonb '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]' @* '$[*].keyvalue()';
select jsonb '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]' @* 'strict $.keyvalue()';
select jsonb '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]' @* 'lax $.keyvalue()';

select jsonb 'null' @* '$.double()';
select jsonb 'true' @* '$.double()';
select jsonb '[]' @* '$.double()';
select jsonb '[]' @* 'strict $.double()';
select jsonb '{}' @* '$.double()';
select jsonb '1.23' @* '$.double()';
select jsonb '"1.23"' @* '$.double()';
select jsonb '"1.23aaa"' @* '$.double()';

select jsonb '["", "a", "abc", "abcabc"]' @* '$[*] ? (@ starts with "abc")';
select jsonb '["", "a", "abc", "abcabc"]' @* 'strict $ ? (@[*] starts with "abc")';
select jsonb '["", "a", "abd", "abdabc"]' @* 'strict $ ? (@[*] starts with "abc")';
select jsonb '["abc", "abcabc", null, 1]' @* 'strict $ ? (@[*] starts with "abc")';
select jsonb '["abc", "abcabc", null, 1]' @* 'strict $ ? ((@[*] starts with "abc") is unknown)';
select jsonb '[[null, 1, "abc", "abcabc"]]' @* 'lax $ ? (@[*] starts with "abc")';
select jsonb '[[null, 1, "abd", "abdabc"]]' @* 'lax $ ? ((@[*] starts with "abc") is unknown)';
select jsonb '[null, 1, "abd", "abdabc"]' @* 'lax $[*] ? ((@ starts with "abc") is unknown)';

select jsonb '[null, 1, "abc", "abd", "aBdC", "abdacb", "babc"]' @* 'lax $[*] ? (@ like_regex "^ab.*c")';
select jsonb '[null, 1, "abc", "abd", "aBdC", "abdacb", "babc"]' @* 'lax $[*] ? (@ like_regex "^ab.*c" flag "i")';

select jsonb 'null' @* '$.datetime()';
select jsonb 'true' @* '$.datetime()';
select jsonb '[]' @* '$.datetime()';
select jsonb '[]' @* 'strict $.datetime()';
select jsonb '{}' @* '$.datetime()';
select jsonb '""' @* '$.datetime()';

-- Standard extension: UNIX epoch to timestamptz
select jsonb '0' @* '$.datetime()';
select jsonb '0' @* '$.datetime().type()';
select jsonb '1490216035.5' @* '$.datetime()';

select jsonb '"10-03-2017"' @*       '$.datetime("dd-mm-yyyy")';
select jsonb '"10-03-2017"' @*       '$.datetime("dd-mm-yyyy").type()';
select jsonb '"10-03-2017 12:34"' @* '$.datetime("dd-mm-yyyy")';
select jsonb '"10-03-2017 12:34"' @* '$.datetime("dd-mm-yyyy").type()';

select jsonb '"10-03-2017 12:34"' @* '       $.datetime("dd-mm-yyyy HH24:MI").type()';
select jsonb '"10-03-2017 12:34 +05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM").type()';
select jsonb '"12:34:56"' @*                '$.datetime("HH24:MI:SS").type()';
select jsonb '"12:34:56 +05:20"' @*         '$.datetime("HH24:MI:SS TZH:TZM").type()';

set time zone '+00';

select jsonb '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI")';
select jsonb '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select jsonb '"10-03-2017 12:34 +05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select jsonb '"10-03-2017 12:34 -05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select jsonb '"10-03-2017 12:34 +05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select jsonb '"10-03-2017 12:34 -05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select jsonb '"12:34"' @*       '$.datetime("HH24:MI")';
select jsonb '"12:34"' @*       '$.datetime("HH24:MI TZH")';
select jsonb '"12:34 +05"' @*    '$.datetime("HH24:MI TZH")';
select jsonb '"12:34 -05"' @*    '$.datetime("HH24:MI TZH")';
select jsonb '"12:34 +05:20"' @* '$.datetime("HH24:MI TZH:TZM")';
select jsonb '"12:34 -05:20"' @* '$.datetime("HH24:MI TZH:TZM")';

set time zone '+10';

select jsonb '"10-03-2017 12:34"' @*       '$.datetime("dd-mm-yyyy HH24:MI")';
select jsonb '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select jsonb '"10-03-2017 12:34 +05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select jsonb '"10-03-2017 12:34 -05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select jsonb '"10-03-2017 12:34 +05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select jsonb '"10-03-2017 12:34 -05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select jsonb '"12:34"' @*        '$.datetime("HH24:MI")';
select jsonb '"12:34"' @*        '$.datetime("HH24:MI TZH")';
select jsonb '"12:34 +05"' @*    '$.datetime("HH24:MI TZH")';
select jsonb '"12:34 -05"' @*    '$.datetime("HH24:MI TZH")';
select jsonb '"12:34 +05:20"' @* '$.datetime("HH24:MI TZH:TZM")';
select jsonb '"12:34 -05:20"' @* '$.datetime("HH24:MI TZH:TZM")';

set time zone default;

select jsonb '"2017-03-10"' @* '$.datetime().type()';
select jsonb '"2017-03-10"' @* '$.datetime()';
select jsonb '"2017-03-10 12:34:56"' @* '$.datetime().type()';
select jsonb '"2017-03-10 12:34:56"' @* '$.datetime()';
select jsonb '"2017-03-10 12:34:56 +3"' @* '$.datetime().type()';
select jsonb '"2017-03-10 12:34:56 +3"' @* '$.datetime()';
select jsonb '"2017-03-10 12:34:56 +3:10"' @* '$.datetime().type()';
select jsonb '"2017-03-10 12:34:56 +3:10"' @* '$.datetime()';
select jsonb '"12:34:56"' @* '$.datetime().type()';
select jsonb '"12:34:56"' @* '$.datetime()';
select jsonb '"12:34:56 +3"' @* '$.datetime().type()';
select jsonb '"12:34:56 +3"' @* '$.datetime()';
select jsonb '"12:34:56 +3:10"' @* '$.datetime().type()';
select jsonb '"12:34:56 +3:10"' @* '$.datetime()';

set time zone '+00';

-- date comparison
select jsonb
	'["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]' @*
	'$[*].datetime() ? (@ == "10.03.2017".datetime("dd.mm.yyyy"))';
select jsonb
	'["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]' @*
	'$[*].datetime() ? (@ >= "10.03.2017".datetime("dd.mm.yyyy"))';
select jsonb
	'["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]' @*
	'$[*].datetime() ? (@ <  "10.03.2017".datetime("dd.mm.yyyy"))';

-- time comparison
select jsonb
	'["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]' @*
	'$[*].datetime() ? (@ == "12:35".datetime("HH24:MI"))';
select jsonb
	'["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]' @*
	'$[*].datetime() ? (@ >= "12:35".datetime("HH24:MI"))';
select jsonb
	'["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]' @*
	'$[*].datetime() ? (@ <  "12:35".datetime("HH24:MI"))';

-- timetz comparison
select jsonb
	'["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]' @*
	'$[*].datetime() ? (@ == "12:35 +1".datetime("HH24:MI TZH"))';
select jsonb
	'["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]' @*
	'$[*].datetime() ? (@ >= "12:35 +1".datetime("HH24:MI TZH"))';
select jsonb
	'["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]' @*
	'$[*].datetime() ? (@ <  "12:35 +1".datetime("HH24:MI TZH"))';

-- timestamp comparison
select jsonb
	'["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]' @*
	'$[*].datetime() ? (@ == "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))';
select jsonb
	'["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]' @*
	'$[*].datetime() ? (@ >= "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))';
select jsonb
	'["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]' @*
	'$[*].datetime() ? (@ < "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))';

-- timestamptz comparison
select jsonb
	'["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]' @*
	'$[*].datetime() ? (@ == "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))';
select jsonb
	'["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]' @*
	'$[*].datetime() ? (@ >= "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))';
select jsonb
	'["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]' @*
	'$[*].datetime() ? (@ < "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))';

set time zone default;

-- jsonpath operators

SELECT jsonb '[{"a": 1}, {"a": 2}]' @* '$[*]';
SELECT jsonb '[{"a": 1}, {"a": 2}]' @* '$[*] ? (@.a > 10)';
SELECT jsonb '[{"a": 1}, {"a": 2}]' @* '[$[*].a]';

SELECT jsonb '[{"a": 1}, {"a": 2}]' @? '$[*].a ? (@ > 1)';
SELECT jsonb '[{"a": 1}, {"a": 2}]' @? '$[*] ? (@.a > 2)';

SELECT jsonb '[{"a": 1}, {"a": 2}]' @~ '$[*].a > 1';
SELECT jsonb '[{"a": 1}, {"a": 2}]' @~ '$[*].a > 2';

-- extension: path sequences
select jsonb '[1,2,3,4,5]' @* '10, 20, $[*], 30';
select jsonb '[1,2,3,4,5]' @* 'lax    10, 20, $[*].a, 30';
select jsonb '[1,2,3,4,5]' @* 'strict 10, 20, $[*].a, 30';
select jsonb '[1,2,3,4,5]' @* '-(10, 20, $[1 to 3], 30)';
select jsonb '[1,2,3,4,5]' @* 'lax (10, 20.5, $[1 to 3], "30").double()';
select jsonb '[1,2,3,4,5]' @* '$[(0, $[*], 5) ? (@ == 3)]';
select jsonb '[1,2,3,4,5]' @* '$[(0, $[*], 3) ? (@ == 3)]';

-- extension: array constructors
select jsonb '[1, 2, 3]' @* '[]';
select jsonb '[1, 2, 3]' @* '[1, 2, $[*], 4, 5]';
select jsonb '[1, 2, 3]' @* '[1, 2, $[*], 4, 5][*]';
select jsonb '[1, 2, 3]' @* '[(1, (2, $[*])), (4, 5)]';
select jsonb '[1, 2, 3]' @* '[[1, 2], [$[*], 4], 5, [(1,2)?(@ > 5)]]';
select jsonb '[1, 2, 3]' @* 'strict [1, 2, $[*].a, 4, 5]';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '[$[*][*] ? (@ > 3)]';

-- extension: object constructors
select jsonb '[1, 2, 3]' @* '{}';
select jsonb '[1, 2, 3]' @* '{a: 2 + 3, "b": [$[*], 4, 5]}';
select jsonb '[1, 2, 3]' @* '{a: 2 + 3, "b": [$[*], 4, 5]}.*';
select jsonb '[1, 2, 3]' @* '{a: 2 + 3, "b": [$[*], 4, 5]}[*]';
select jsonb '[1, 2, 3]' @* '{a: 2 + 3, "b": ($[*], 4, 5)}';
select jsonb '[1, 2, 3]' @* '{a: 2 + 3, "b": {x: $, y: $[1] > 2, z: "foo"}}';

-- extension: object subscripting
select jsonb '{"a": 1}' @? '$["a"]';
select jsonb '{"a": 1}' @? '$["b"]';
select jsonb '{"a": 1}' @? 'strict $["b"]';
select jsonb '{"a": 1}' @? '$["b", "a"]';

select jsonb '{"a": 1}' @* '$["a"]';
select jsonb '{"a": 1}' @* 'strict $["b"]';
select jsonb '{"a": 1}' @* 'lax $["b"]';
select jsonb '{"a": 1, "b": 2}' @* 'lax $["b", "c", "b", "a", 0 to 3]';

select jsonb 'null' @* '{"a": 1}["a"]';
select jsonb 'null' @* '{"a": 1}["b"]';

-- extension: outer item reference (@N)
select jsonb '[2,4,1,5,3]' @* '$[*] ? (!exists($[*] ? (@ < @1)))';
select jsonb '[2,4,1,5,3]' @* '$[*] ? (!exists($[*] ? (@ > @1)))';
select jsonb '[2,4,1,5,3]' @* 'strict $ ? (@[*] ? (@ < @1[1]) > 2)';
select jsonb '[2,4,1,5,3]' @* 'strict $ ? (@[*] ? (@ < @1[1]) > 3)';
select jsonb '[2,4,1,5,3]' @* 'strict $ ? (@[*] ? (@ ? (@ ? (@ < @3[1]) > @2[0]) > @1[0]) > 2)';
select jsonb '[2,4,1,5,3]' @* 'strict $ ? (@[*] ? (@ ? (@ ? (@ < @3[2]) > @2[0]) > @1[0]) > 2)';

-- extension: including subpaths into result
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a[*].b)';
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a[*]).b';
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.a.([*].b)';
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a)[*].b';
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.a[*].(b)';
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a)[*].(b)';
select jsonb '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a.[0 to 1].b)';

-- extension: user-defined functions and item methods
-- array_map(jsonpath_fcxt, jsonb) function created in create_function_1.sql
-- array_map() item method
select jsonb '1' @* 'strict $.array_map(x => x + 10)';
select jsonb '1' @* 'lax $.array_map(x => x + 10)';
select jsonb '[1, 2, 3]' @* '$.array_map(x => x + 10)';
select jsonb '[1, 2, 3]' @* '$.array_map(x => x + 10)[*]';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '$.array_map(a => a.array_map(x => x + 10))';

-- array_map() function
select jsonb '1' @* 'strict array_map($, x => x + 10)';
select jsonb '1' @* 'lax array_map($, x => x + 10)';
select jsonb '[3, 4, 5]' @* 'array_map($[*], (x, i) => x + i * 10)';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* 'array_map($[*], x => [array_map(x[*], x => x + 10)])';

-- extension: custom operators and type casts
select jsonb '"aaa"' @* '$::text || "bbb"::text || $::text';
select jsonb '"aaa"' @* '$::text || "bbb" || $';
select jsonb '[null, true, 1, "aaa",  {"a": 1}, [1, 2]]' @* '$.array_map(x => x::text || "xyz"::text)';

select jsonb '123.45' @* '$::int4';
select jsonb '123.45' @* '$::float4';
select jsonb '123.45' @* '$::text';
select jsonb '123.45' @* '$::text::int4';
select jsonb '123.45' @* '$::text::float4';
select jsonb '123.45' @* '$::text::float4::int4';
select jsonb '4000000000' @* '$::int8';

select jsonb '[123.45, null, 0.67]' @* '$[*]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::text';
select jsonb '[123.45, null, 0.67, "8.9"]' @* '$[*]::text::float4::int4';


select jsonb '[123.45, 0.67]' @* '$[*]::int4 > $[0]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::int4 > $[0]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::int4 > $[1]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::int4 > $[2]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[0]::int4 > $[*]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::int4 > $[2]::text::float4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::text::float4 > $[2]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::text::float4 > $[2]::text::float4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::int4 > $[0 to 1]::int4';
select jsonb '[123.45, null, 0.67]' @* '$[*]::int4 > $[1 to 2]::int4';
select jsonb '[123.45, 100000.2, 10000.67, "1"]' @* '$[0]::int8 > $[*]::int4::int8';

select jsonb '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[*] -> "a"::text';
select jsonb '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[0] -> "a"::text';
select jsonb '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[1] -> $[0].a::text';
select jsonb '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[0] \? "a"::text';
select jsonb '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[*] \? "b"::text';
select jsonb '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[*] \? "c"::text';
select jsonb '[{"a": "b"}, {"b": [1, "2"]}, null, 1]' @* '$[*] ? (@ \? "a"::text)';

select jsonb '[1, "t", 0, "f", null]' @* '$[*] ? (@::int4)';
select jsonb '[1, "t", 0, "f", null]' @* '$[*] ? (@::bool)';
select jsonb '[1, "t", 0, "f", null]' @* '$[*] ? (!(@::bool))';
select jsonb '[1, "t", 0, "f", null]' @* '$[*] ? (@::bool == false::bool)';
select jsonb '[1, "t", 0, "f", null]' @* '$[*] ? (@::bool || !(@::bool))';

select jsonb '[1, 2, 3]' @* '$[*] ? (@::int4 > 1::int4)';

select jsonb '"str"' @* '$::json';
select jsonb '"str"' @* '$::jsonb';

select jsonb '1' @* '$ ? (@ \@> 1)';
select jsonb '1' @* '$ ? (@ \@> 2)';
