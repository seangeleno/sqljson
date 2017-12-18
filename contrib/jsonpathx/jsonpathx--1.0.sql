/* contrib/jsonpathx/jsonpathx--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION jsonpathx" to load this file. \quit

CREATE FUNCTION map(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_map_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION map(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_map_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION flatmap(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_flatmap_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION flatmap(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_flatmap_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION reduce(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_reduce_jsonb' 
LANGUAGE C STRICT STABLE;

CREATE FUNCTION reduce(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_reduce_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION fold(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_fold_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION fold(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_fold_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION foldl(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_foldl_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION foldl(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_foldl_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION foldr(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_foldr_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION foldr(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_foldr_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION min(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_min_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION min(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_min_json'
LANGUAGE C STRICT STABLE;


CREATE FUNCTION max(jsonpath_fcxt, jsonb)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_max_jsonb'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION max(jsonpath_fcxt, json) 
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_max_json'
LANGUAGE C STRICT STABLE;

