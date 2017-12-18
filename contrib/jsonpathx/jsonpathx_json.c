/*-------------------------------------------------------------------------
 *
 * jsonpathx_json.c
 *	   jsonpathx support for json type.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	   contrib/jsonpathx/jsonpathx_json.c
 *
 *-------------------------------------------------------------------------
 */
#define JSONPATHX_JSON_C

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

#include "utils/jsonpath_json.h"

#include "jsonpathx.c"
