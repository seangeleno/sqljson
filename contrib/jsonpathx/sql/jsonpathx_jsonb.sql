CREATE EXTENSION jsonpathx;

-- map item method
select jsonb '1' @* 'strict $.map(x => x + 10)';
select jsonb '1' @* 'lax $.map(x => x + 10)';
select jsonb '[1, 2, 3]' @* '$.map(x => x + 10)';
select jsonb '[1, 2, 3]' @* '$.map(x => x + 10)[*]';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '$.map(a => a.map(x => x + 10))';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '$.flatmap(a => a.map(a => a + 10))';

-- map function
select jsonb '1' @* 'strict map($, x => x + 10)';
select jsonb '1' @* 'lax map($, x => x + 10)';
select jsonb '[1, 2, 3]' @* 'map($[*], x => x + 10)';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* 'map($[*], x => [map(x[*], x => x + 10)])';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* 'flatmap($[*], a => map(a[*], x => x + 10))';

-- reduce/fold item methods
select jsonb '1' @* 'strict $.reduce((x, y) => x + y)';
select jsonb '1' @* 'lax $.reduce((x, y) => x + y)';
select jsonb '1' @* 'strict $.fold((x, y) => x + y, 10)';
select jsonb '1' @* 'lax $.fold((x, y) => x + y, 10)';
select jsonb '[1, 2, 3]' @* '$.reduce((x, y) => x + y)';
select jsonb '[1, 2, 3]' @* '$.fold((x, y) => x + y, 100)';
select jsonb '[]' @* '$.reduce((x, y) => x + y)';
select jsonb '[]' @* '$.fold((x, y) => x + y, 100)';
select jsonb '[1]' @* '$.reduce((x, y) => x + y)';
select jsonb '[1, 2, 3]' @* '$.foldl((x, y) => [x, y], [])';
select jsonb '[1, 2, 3]' @* '$.foldr((x, y) => [y, x], [])';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '$.fold((x, y) => x + y.fold((a, b) => a + b, 100), 1000)';

-- reduce/fold functions
select jsonb '1' @* 'strict reduce($, (x, y) => x + y)';
select jsonb '1' @* 'lax reduce($, (x, y) => x + y)';
select jsonb '1' @* 'strict fold($, (x, y) => x + y, 10)';
select jsonb '1' @* 'lax fold($, (x, y) => x + y, 10)';
select jsonb '[1, 2, 3]' @* 'reduce($[*], (x, y) => x + y)';
select jsonb '[1, 2, 3]' @* 'fold($[*], (x, y) => x + y, 100)';
select jsonb '[]' @* 'reduce($[*], (x, y) => x + y)';
select jsonb '[]' @* 'fold($[*], (x, y) => x + y, 100)';
select jsonb '[1]' @* 'reduce($[*], (x, y) => x + y)';
select jsonb '[1, 2, 3]' @* 'foldl($[*], (x, y) => [x, y], [])';
select jsonb '[1, 2, 3]' @* 'foldr($[*], (x, y) => [y, x], [])';
select jsonb '[[1, 2], [3, 4, 5], [], [6, 7]]' @* 'fold($[*], (x, y) => x + y.fold((a, b) => a + b, 100), 1000)';

-- min/max item methods
select jsonb '1' @* 'strict $.min()';
select jsonb '1' @* 'lax $.min()';
select jsonb '[]' @* '$.min()';
select jsonb '[]' @* '$.max()';
select jsonb '[null]' @* '$.min()';
select jsonb '[null]' @* '$.max()';
select jsonb '[1, 2, 3]' @* '$.min()';
select jsonb '[1, 2, 3]' @* '$.max()';
select jsonb '[2, 3, 5, null, 1, 4, null]' @* '$.min()';
select jsonb '[2, 3, 5, null, 1, 4, null]' @* '$.max()';
select jsonb '["aa", null, "a", "bbb"]' @* '$.min()';
select jsonb '["aa", null, "a", "bbb"]' @* '$.max()';
select jsonb '[1, null, "2"]' @* '$.max()';

-- min/max functions
select jsonb '1' @* 'strict min($)';
select jsonb '1' @* 'lax min($)';
select jsonb '[]' @* 'min($[*])';
select jsonb '[]' @* 'max($[*])';
select jsonb '[null]' @* 'min($[*])';
select jsonb '[null]' @* 'max($[*])';
select jsonb '[1, 2, 3]' @* 'min($[*])';
select jsonb '[1, 2, 3]' @* 'max($[*])';
select jsonb '[2, 3, 5, null, 1, 4, null]' @* 'min($[*])';
select jsonb '[2, 3, 5, null, 1, 4, null]' @* 'max($[*])';
select jsonb '["aa", null, "a", "bbb"]' @* 'min($[*])';
select jsonb '["aa", null, "a", "bbb"]' @* 'max($[*])';
select jsonb '[1, null, "2"]' @* 'max($[*])';

-- tests for simplified variable-based lambda syntax
select jsonb '[1, 2, 3]' @* '$.map($1 + 100)';
select jsonb '[1, 2, 3]' @* 'map($[*], $1 + 100)';
select jsonb '[1, 2, 3]' @* '$.reduce($1 + $2)';
select jsonb '[1, 2, 3]' @* 'reduce($[*], $1 + $2)';
select jsonb '[1, 2, 3]' @* '$.fold($1 + $2, 100)';
select jsonb '[1, 2, 3]' @* 'fold($[*], $1 + $2, 100)';

-- more complex tests
select jsonb '[0,1,2,3,4,5,6,7,8,9]' @* '$.map((x,i,a) => {n: i, sum: reduce(a[0 to i], (x,y) => x + y)})[*]';
select jsonb '[0,1,2,3,4,5,6,7,8,9]' @* '$.fold((x,y,i,a) => [x[*], {n:y, s: [a[0 to i]].reduce($1+$2)}], [])[*]';
select jsonb '[0,1,2,3,4,5,6,7,8,9]' @* '$.fold((x,y) => [y,y,y].map((a) => a + y).reduce((x,y)=>x+y) + x * 100, 0)';

DROP EXTENSION jsonpathx;
