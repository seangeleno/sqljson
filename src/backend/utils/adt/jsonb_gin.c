/*-------------------------------------------------------------------------
 *
 * jsonb_gin.c
 *	 GIN support functions for jsonb
 *
 * Copyright (c) 2014-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_gin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "access/gin.h"
#include "access/hash.h"
#include "access/stratnum.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/jsonpath.h"
#include "utils/varlena.h"

typedef struct PathHashStack
{
	uint32		hash;
	struct PathHashStack *parent;
} PathHashStack;

typedef enum { eOr, eAnd, eEntry } JsonPathNodeType;

typedef struct JsonPathNode
{
	JsonPathNodeType type;
	union
	{
		int			nargs;
		int			entry;
	} val;
	struct JsonPathNode *args[FLEXIBLE_ARRAY_MEMBER];
} JsonPathNode;

typedef struct JsonPathExtractionContext
{
	Datum	   *entries;
	int32		nentries;
	int32		nallocated;
	void	 *(*addKey)(void *path, char *key, int len);
	bool		pathOps;
	bool		lax;
} JsonPathExtractionContext;

typedef struct JsonPathContext
{
	void	   *path;
	JsonPathItemType last;
} JsonPathContext;

static Datum make_text_key(char flag, const char *str, int len);
static Datum make_scalar_key(const JsonbValue *scalarVal, bool is_key);

static JsonPathNode *gin_extract_jsonpath_expr_recursive(
	JsonPathExtractionContext *cxt, JsonPathItem *jsp, bool not,
	JsonPathContext path);

/*
 *
 * jsonb_ops GIN opclass support functions
 *
 */

Datum
gin_compare_jsonb(PG_FUNCTION_ARGS)
{
	text	   *arg1 = PG_GETARG_TEXT_PP(0);
	text	   *arg2 = PG_GETARG_TEXT_PP(1);
	int32		result;
	char	   *a1p,
			   *a2p;
	int			len1,
				len2;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	/* Compare text as bttextcmp does, but always using C collation */
	result = varstr_cmp(a1p, len1, a2p, len2, C_COLLATION_OID);

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_INT32(result);
}

Datum
gin_extract_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = (Jsonb *) PG_GETARG_JSONB_P(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	int			total = 2 * JB_ROOT_COUNT(jb);
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken r;
	int			i = 0;
	Datum	   *entries;

	/* If the root level is empty, we certainly have no keys */
	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	/* Otherwise, use 2 * root count as initial estimate of result size */
	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = JsonbIteratorInit(&jb->root);

	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		/* Since we recurse into the object, we might need more space */
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch (r)
		{
			case WJB_KEY:
				entries[i++] = make_scalar_key(&v, true);
				break;
			case WJB_ELEM:
				/* Pretend string array elements are keys, see jsonb.h */
				entries[i++] = make_scalar_key(&v, (v.type == jbvString));
				break;
			case WJB_VALUE:
				entries[i++] = make_scalar_key(&v, false);
				break;
			default:
				/* we can ignore structural items */
				break;
		}
	}

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

/*
 * Extract JSON path into the 'pathcxt' with filters.
 * Returns true iff this path is supported by the index opclass.
 */
static bool
gin_extract_jsonpath_path(JsonPathExtractionContext *cxt, JsonPathItem *jsp,
						  JsonPathContext *pathcxt, List **filters)
{
	JsonPathItem next;

	for (;;)
	{
		/* save the type of the last item in the path */
		if (jsp->type != jpiFilter && jsp->type != jpiCurrent)
			pathcxt->last = jsp->type;

		switch (jsp->type)
		{
			case jpiRoot:
				pathcxt->path = NULL;
				break;

			case jpiCurrent:
				break;

			case jpiKey:
				pathcxt->path = cxt->addKey(pathcxt->path,
											jsp->content.value.data,
											jsp->content.value.datalen);
				break;

			case jpiIndexArray:
			case jpiAnyArray:
				break;

			case jpiAny:
			case jpiAnyKey:
				if (cxt->pathOps)
					/* jsonb_path_ops doesn't support wildcard paths */
					return false;
				break;

			case jpiFilter:
				{
					JsonPathItem arg;
					JsonPathNode *filter;

					jspGetArg(jsp, &arg);

					filter = gin_extract_jsonpath_expr_recursive(cxt, &arg, false, *pathcxt);

					if (filter)
						*filters = lappend(*filters, filter);

					break;
				}

			default:
				/* other path items (like item methods) are not supported */
				return false;
		}

		if (!jspGetNext(jsp, &next))
			break;

		jsp = &next;
	}

	return true;
}

/* Append an entry node to the global entry list. */
static inline JsonPathNode *
gin_jsonpath_make_entry_node(JsonPathExtractionContext *cxt, Datum entry)
{
	JsonPathNode *node = palloc(offsetof(JsonPathNode, args));

	if (cxt->nentries >= cxt->nallocated)
	{
		if (cxt->entries)
		{
			cxt->nallocated *= 2;
			cxt->entries = repalloc(cxt->entries,
									sizeof(cxt->entries[0]) * cxt->nallocated);
		}
		else
		{
			cxt->nallocated = 8;
			cxt->entries = palloc(sizeof(cxt->entries[0]) * cxt->nallocated);
		}
	}

	node->type = eEntry;
	node->val.entry = cxt->nentries;

	cxt->entries[cxt->nentries++] = entry;

	return node;
}

static inline JsonPathNode *
gin_jsonpath_make_expr_node(JsonPathNodeType type, int nargs)
{
	JsonPathNode *node = palloc(offsetof(JsonPathNode, args) +
								sizeof(node->args[0]) * nargs);

	node->type = type;
	node->val.nargs = nargs;

	return node;
}

static inline JsonPathNode *
gin_jsonpath_make_expr_node_from_list(JsonPathNodeType type, List *args)
{
	JsonPathNode *node = gin_jsonpath_make_expr_node(type, list_length(args));
	ListCell   *lc;
	int			i = 0;

	foreach(lc, args)
		node->args[i++] = lfirst(lc);

	return node;
}

/*
 * Extract node from the EXISTS/equality-comparison jsonpath expression.  If
 * 'scalar' is not NULL this is equality-comparsion, otherwise this is
 * EXISTS-predicate. The current path is passed in 'pathcxt'.
 */
static JsonPathNode *
gin_extract_jsonpath_node(JsonPathExtractionContext *cxt, JsonPathItem *jsp,
						  JsonPathContext pathcxt, JsonbValue *scalar)
{
	JsonPathNode *node;
	List	   *filters = NIL;
	ListCell   *lc;

	if (!gin_extract_jsonpath_path(cxt, jsp, &pathcxt, &filters))
		return NULL;

	if (cxt->pathOps)
	{
		if (scalar)
		{
			uint32		hash = (uint32)(uintptr_t) pathcxt.path;

			JsonbHashScalarValue(scalar, &hash);
			node = gin_jsonpath_make_entry_node(cxt, UInt32GetDatum(hash));
		}
		else
			node = NULL; /* jsonb_path_ops doesn't support EXISTS queries */
	}
	else
	{
		List	   *entries = pathcxt.path;
		List	   *nodes = NIL;

		if (scalar)
		{
			bool lastIsArrayAccessor =
				pathcxt.last == jpiIndexArray ||
				pathcxt.last == jpiAnyArray ? GIN_TRUE :
				pathcxt.last == jpiAny ? GIN_MAYBE : GIN_FALSE;

			/*
			 * Create OR-node when the string scalar can be matched as a key
			 * and a non-key. It is possible in lax mode where arrays are
			 * automatically unwrapped, or in strict mode for jpiAny items.
			 */
			if (scalar->type == jbvString &&
				(cxt->lax || lastIsArrayAccessor == GIN_MAYBE))
			{
				node = gin_jsonpath_make_expr_node(eOr, 2);
				node->args[0] = gin_jsonpath_make_entry_node(cxt,
												make_scalar_key(scalar, true));
				node->args[1] = gin_jsonpath_make_entry_node(cxt,
												make_scalar_key(scalar, false));
			}
			else
			{
				Datum entry = make_scalar_key(scalar,
											  scalar->type == jbvString &&
											  lastIsArrayAccessor == GIN_TRUE);

				node = gin_jsonpath_make_entry_node(cxt, entry);
			}

			nodes = lappend(nodes, node);
		}

		foreach(lc, entries)
			nodes = lappend(nodes, gin_jsonpath_make_entry_node(cxt,
												PointerGetDatum(lfirst(lc))));

		if (list_length(nodes) > 0)
			node = gin_jsonpath_make_expr_node_from_list(eAnd, nodes);
		else
			node = NULL;	/* need full scan for EXISTS($) queries */
	}

	if (list_length(filters) <= 0)
		return node;

	/* construct AND-node for path with filters */
	if (node)
		filters = lcons(node, filters);

	return gin_jsonpath_make_expr_node_from_list(eAnd, filters);
}

/* Extract nodes from the boolean jsonpath expression. */
static JsonPathNode *
gin_extract_jsonpath_expr_recursive(JsonPathExtractionContext *cxt,
									JsonPathItem *jsp, bool not,
									JsonPathContext path)
{
	check_stack_depth();

	switch (jsp->type)
	{
		case jpiAnd:
		case jpiOr:
			{
				JsonPathItem arg;
				JsonPathNode *larg;
				JsonPathNode *rarg;
				JsonPathNode *node;
				JsonPathNodeType type;

				jspGetLeftArg(jsp, &arg);
				larg = gin_extract_jsonpath_expr_recursive(cxt, &arg, not, path);

				jspGetRightArg(jsp, &arg);
				rarg = gin_extract_jsonpath_expr_recursive(cxt, &arg, not, path);

				if (!larg || !rarg)
				{
					if (jsp->type == jpiOr)
						return NULL;
					return larg ? larg : rarg;
				}

				type = not ^ (jsp->type == jpiAnd) ? eAnd : eOr;
				node = gin_jsonpath_make_expr_node(type, 2);
				node->args[0] = larg;
				node->args[1] = rarg;

				return node;
			}

		case jpiNot:
			{
				JsonPathItem arg;

				jspGetArg(jsp, &arg);

				return gin_extract_jsonpath_expr_recursive(cxt, &arg, !not, path);
			}

		case jpiExists:
			{
				JsonPathItem arg;

				if (not)
					return false;

				jspGetArg(jsp, &arg);

				return gin_extract_jsonpath_node(cxt, &arg, path, NULL);
			}

		case jpiEqual:
			{
				JsonPathItem leftItem;
				JsonPathItem rightItem;
				JsonPathItem *pathItem;
				JsonPathItem *scalarItem;
				JsonbValue	scalar;

				if (not)
					return NULL;

				jspGetLeftArg(jsp, &leftItem);
				jspGetRightArg(jsp, &rightItem);

				if (jspIsScalar(leftItem.type))
				{
					scalarItem = &leftItem;
					pathItem = &rightItem;
				}
				else if (jspIsScalar(rightItem.type))
				{
					scalarItem = &rightItem;
					pathItem = &leftItem;
				}
				else
					return NULL; /* at least one operand should be a scalar */

				switch (scalarItem->type)
				{
					case jpiNull:
						scalar.type = jbvNull;
						break;
					case jpiBool:
						scalar.type = jbvBool;
						scalar.val.boolean = !!*scalarItem->content.value.data;
						break;
					case jpiNumeric:
						scalar.type = jbvNumeric;
						scalar.val.numeric =
							(Numeric) scalarItem->content.value.data;
						break;
					case jpiString:
						scalar.type = jbvString;
						scalar.val.string.val = scalarItem->content.value.data;
						scalar.val.string.len = scalarItem->content.value.datalen;
						break;
					default:
						elog(ERROR, "invalid scalar jsonpath item type: %d",
							 scalarItem->type);
						return NULL;
				}

				return gin_extract_jsonpath_node(cxt, pathItem, path, &scalar);
			}

		default:
			return NULL;
	}
}

/* Append key name to a path. */
static void *
gin_jsonb_ops_add_key(void *path, char *key, int len)
{
	return lappend((List *) path, DatumGetPointer(
									make_text_key(JGINFLAG_KEY, key, len)));
}

/* Combine existing path hash with next key hash. */
static void *
gin_jsonb_path_ops_add_key(void *path, char *key, int len)
{
	JsonbValue 	jbv;
	uint32		hash = (uint32)(uintptr_t) path;

	jbv.type = jbvString;
	jbv.val.string.val = key;
	jbv.val.string.len = len;

	JsonbHashScalarValue(&jbv, &hash);

	return (void *)(uintptr_t) hash;
}

static Datum *
gin_extract_jsonpath_query(JsonPath *jp, StrategyNumber strat, bool pathOps,
						   int32 *nentries, Pointer **extra_data)
{
	JsonPathExtractionContext cxt = { 0 };
	JsonPathItem root;
	JsonPathNode *node;
	JsonPathContext path = { NULL, GIN_FALSE };

	cxt.addKey = pathOps ? gin_jsonb_path_ops_add_key : gin_jsonb_ops_add_key;
	cxt.pathOps = pathOps;
	cxt.lax = (jp->header & JSONPATH_LAX) != 0;

	jspInit(&root, jp);

	node = strat == JsonbJsonpathExistsStrategyNumber
		? gin_extract_jsonpath_node(&cxt, &root, path, NULL)
		: gin_extract_jsonpath_expr_recursive(&cxt, &root, false, path);

	if (!node)
	{
		*nentries = 0;
		return NULL;
	}

	*nentries = cxt.nentries;
	*extra_data = palloc(sizeof(**extra_data) * cxt.nentries);
	**extra_data = (Pointer) node;

	return cxt.entries;
}

static GinTernaryValue
gin_execute_jsonpath(JsonPathNode *node, GinTernaryValue *check)
{
	GinTernaryValue	res;
	GinTernaryValue	v;
	int			i;

	switch (node->type)
	{
		case eAnd:
			res = GIN_TRUE;
			for (i = 0; i < node->val.nargs; i++)
			{
				v = gin_execute_jsonpath(node->args[i], check);
				if (v == GIN_FALSE)
					return GIN_FALSE;
				else if (v == GIN_MAYBE)
					res = GIN_MAYBE;
			}
			return res;

		case eOr:
			res = GIN_FALSE;
			for (i = 0; i < node->val.nargs; i++)
			{
				v = gin_execute_jsonpath(node->args[i], check);
				if (v == GIN_TRUE)
					return GIN_TRUE;
				else if (v == GIN_MAYBE)
					res = GIN_MAYBE;
			}
			return res;

		case eEntry:
			return check[node->val.entry] ? GIN_MAYBE : GIN_FALSE;

		default:
			elog(ERROR, "invalid jsonpath gin node type: %d", node->type);
			return GIN_FALSE;
	}
}

Datum
gin_extract_jsonb_query(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/* Query is a jsonb, so just apply gin_extract_jsonb... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_jsonb,
												PG_GETARG_DATUM(0),
												PointerGetDatum(nentries)));
		/* ...although "contains {}" requires a full index scan */
		if (*nentries == 0)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		/* Query is a text string, which we treat as a key */
		text	   *query = PG_GETARG_TEXT_PP(0);

		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		entries[0] = make_text_key(JGINFLAG_KEY,
								   VARDATA_ANY(query),
								   VARSIZE_ANY_EXHDR(query));
	}
	else if (strategy == JsonbExistsAnyStrategyNumber ||
			 strategy == JsonbExistsAllStrategyNumber)
	{
		/* Query is a text array; each element is treated as a key */
		ArrayType  *query = PG_GETARG_ARRAYTYPE_P(0);
		Datum	   *key_datums;
		bool	   *key_nulls;
		int			key_count;
		int			i,
					j;

		deconstruct_array(query,
						  TEXTOID, -1, false, 'i',
						  &key_datums, &key_nulls, &key_count);

		entries = (Datum *) palloc(sizeof(Datum) * key_count);

		for (i = 0, j = 0; i < key_count; i++)
		{
			/* Nulls in the array are ignored */
			if (key_nulls[i])
				continue;
			entries[j++] = make_text_key(JGINFLAG_KEY,
										 VARDATA(key_datums[i]),
										 VARSIZE(key_datums[i]) - VARHDRSZ);
		}

		*nentries = j;
		/* ExistsAll with no keys should match everything */
		if (j == 0 && strategy == JsonbExistsAllStrategyNumber)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		JsonPath   *jp = PG_GETARG_JSONPATH_P(0);
		Pointer	  **extra_data = (Pointer **) PG_GETARG_POINTER(4);

		entries = gin_extract_jsonpath_query(jp, strategy, false, nentries,
											 extra_data);

		if (!entries)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		entries = NULL;			/* keep compiler quiet */
	}

	PG_RETURN_POINTER(entries);
}

Datum
gin_consistent_jsonb(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * We must always recheck, since we can't tell from the index whether
		 * the positions of the matched items match the structure of the query
		 * object.  (Even if we could, we'd also have to worry about hashed
		 * keys and the index's failure to distinguish keys from string array
		 * elements.)  However, the tuple certainly doesn't match unless it
		 * contains all the query keys.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		/*
		 * Although the key is certainly present in the index, we must recheck
		 * because (1) the key might be hashed, and (2) the index match might
		 * be for a key that's not at top level of the JSON object.  For (1),
		 * we could look at the query key to see if it's hashed and not
		 * recheck if not, but the index lacks enough info to tell about (2).
		 */
		*recheck = true;
		res = true;
	}
	else if (strategy == JsonbExistsAnyStrategyNumber)
	{
		/* As for plain exists, we must recheck */
		*recheck = true;
		res = true;
	}
	else if (strategy == JsonbExistsAllStrategyNumber)
	{
		/* As for plain exists, we must recheck */
		*recheck = true;
		/* ... but unless all the keys are present, we can say "false" */
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		*recheck = true;
		res = nkeys <= 0 ||
			gin_execute_jsonpath((JsonPathNode *) extra_data[0], check) != GIN_FALSE;
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_BOOL(res);
}

Datum
gin_triconsistent_jsonb(PG_FUNCTION_ARGS)
{
	GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);
	Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	GinTernaryValue res = GIN_MAYBE;
	int32		i;

	/*
	 * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE; this
	 * corresponds to always forcing recheck in the regular consistent
	 * function, for the reasons listed there.
	 */
	if (strategy == JsonbContainsStrategyNumber ||
		strategy == JsonbExistsAllStrategyNumber)
	{
		/* All extracted keys must be present */
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_FALSE)
			{
				res = GIN_FALSE;
				break;
			}
		}
	}
	else if (strategy == JsonbExistsStrategyNumber ||
			 strategy == JsonbExistsAnyStrategyNumber)
	{
		/* At least one extracted key must be present */
		res = GIN_FALSE;
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_TRUE ||
				check[i] == GIN_MAYBE)
			{
				res = GIN_MAYBE;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		res = nkeys <= 0 ? GIN_MAYBE :
			gin_execute_jsonpath((JsonPathNode *) extra_data[0], check);
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_GIN_TERNARY_VALUE(res);
}

/*
 *
 * jsonb_path_ops GIN opclass support functions
 *
 * In a jsonb_path_ops index, the GIN keys are uint32 hashes, one per JSON
 * value; but the JSON key(s) leading to each value are also included in its
 * hash computation.  This means we can only support containment queries,
 * but the index can distinguish, for example, {"foo": 42} from {"bar": 42}
 * since different hashes will be generated.
 *
 */

Datum
gin_extract_jsonb_path(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	int			total = 2 * JB_ROOT_COUNT(jb);
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken r;
	PathHashStack tail;
	PathHashStack *stack;
	int			i = 0;
	Datum	   *entries;

	/* If the root level is empty, we certainly have no keys */
	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	/* Otherwise, use 2 * root count as initial estimate of result size */
	entries = (Datum *) palloc(sizeof(Datum) * total);

	/* We keep a stack of partial hashes corresponding to parent key levels */
	tail.parent = NULL;
	tail.hash = 0;
	stack = &tail;

	it = JsonbIteratorInit(&jb->root);

	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		PathHashStack *parent;

		/* Since we recurse into the object, we might need more space */
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch (r)
		{
			case WJB_BEGIN_ARRAY:
			case WJB_BEGIN_OBJECT:
				/* Push a stack level for this object */
				parent = stack;
				stack = (PathHashStack *) palloc(sizeof(PathHashStack));

				/*
				 * We pass forward hashes from outer nesting levels so that
				 * the hashes for nested values will include outer keys as
				 * well as their own keys.
				 *
				 * Nesting an array within another array will not alter
				 * innermost scalar element hash values, but that seems
				 * inconsequential.
				 */
				stack->hash = parent->hash;
				stack->parent = parent;
				break;
			case WJB_KEY:
				/* mix this key into the current outer hash */
				JsonbHashScalarValue(&v, &stack->hash);
				/* hash is now ready to incorporate the value */
				break;
			case WJB_ELEM:
			case WJB_VALUE:
				/* mix the element or value's hash into the prepared hash */
				JsonbHashScalarValue(&v, &stack->hash);
				/* and emit an index entry */
				entries[i++] = UInt32GetDatum(stack->hash);
				/* reset hash for next key, value, or sub-object */
				stack->hash = stack->parent->hash;
				break;
			case WJB_END_ARRAY:
			case WJB_END_OBJECT:
				/* Pop the stack */
				parent = stack->parent;
				pfree(stack);
				stack = parent;
				/* reset hash for next key, value, or sub-object */
				if (stack->parent)
					stack->hash = stack->parent->hash;
				else
					stack->hash = 0;
				break;
			default:
				elog(ERROR, "invalid JsonbIteratorNext rc: %d", (int) r);
		}
	}

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

Datum
gin_extract_jsonb_query_path(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/* Query is a jsonb, so just apply gin_extract_jsonb_path ... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_jsonb_path,
												PG_GETARG_DATUM(0),
												PointerGetDatum(nentries)));

		/* ... although "contains {}" requires a full index scan */
		if (*nentries == 0)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		JsonPath   *jp = PG_GETARG_JSONPATH_P(0);
		Pointer	  **extra_data = (Pointer **) PG_GETARG_POINTER(4);

		entries = gin_extract_jsonpath_query(jp, strategy, true, nentries,
											 extra_data);

		if (!entries)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		entries = NULL;
	}

	PG_RETURN_POINTER(entries);
}

Datum
gin_consistent_jsonb_path(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);
	Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * jsonb_path_ops is necessarily lossy, not only because of hash
		 * collisions but also because it doesn't preserve complete information
		 * about the structure of the JSON object.  Besides, there are some
		 * special rules around the containment of raw scalars in arrays that are
		 * not handled here.  So we must always recheck a match.  However, if not
		 * all of the keys are present, the tuple certainly doesn't match.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		*recheck = true;
		res = nkeys <= 0 ||
			gin_execute_jsonpath((JsonPathNode *) extra_data[0], check);
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_BOOL(res);
}

Datum
gin_triconsistent_jsonb_path(PG_FUNCTION_ARGS)
{
	GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);
	Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	GinTernaryValue res = GIN_MAYBE;
	int32		i;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE; this
		 * corresponds to always forcing recheck in the regular consistent
		 * function, for the reasons listed there.
		 */
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_FALSE)
			{
				res = GIN_FALSE;
				break;
			}
		}
	}
	else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
			 strategy == JsonbJsonpathExistsStrategyNumber)
	{
		res = nkeys <= 0 ? GIN_MAYBE :
			gin_execute_jsonpath((JsonPathNode *) extra_data[0], check);
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_GIN_TERNARY_VALUE(res);
}

/*
 * Construct a jsonb_ops GIN key from a flag byte and a textual representation
 * (which need not be null-terminated).  This function is responsible
 * for hashing overlength text representations; it will add the
 * JGINFLAG_HASHED bit to the flag value if it does that.
 */
static Datum
make_text_key(char flag, const char *str, int len)
{
	text	   *item;
	char		hashbuf[10];

	if (len > JGIN_MAXLENGTH)
	{
		uint32		hashval;

		hashval = DatumGetUInt32(hash_any((const unsigned char *) str, len));
		snprintf(hashbuf, sizeof(hashbuf), "%08x", hashval);
		str = hashbuf;
		len = 8;
		flag |= JGINFLAG_HASHED;
	}

	/*
	 * Now build the text Datum.  For simplicity we build a 4-byte-header
	 * varlena text Datum here, but we expect it will get converted to short
	 * header format when stored in the index.
	 */
	item = (text *) palloc(VARHDRSZ + len + 1);
	SET_VARSIZE(item, VARHDRSZ + len + 1);

	*VARDATA(item) = flag;

	memcpy(VARDATA(item) + 1, str, len);

	return PointerGetDatum(item);
}

/*
 * Create a textual representation of a JsonbValue that will serve as a GIN
 * key in a jsonb_ops index.  is_key is true if the JsonbValue is a key,
 * or if it is a string array element (since we pretend those are keys,
 * see jsonb.h).
 */
static Datum
make_scalar_key(const JsonbValue *scalarVal, bool is_key)
{
	Datum		item;
	char	   *cstr;

	switch (scalarVal->type)
	{
		case jbvNull:
			Assert(!is_key);
			item = make_text_key(JGINFLAG_NULL, "", 0);
			break;
		case jbvBool:
			Assert(!is_key);
			item = make_text_key(JGINFLAG_BOOL,
								 scalarVal->val.boolean ? "t" : "f", 1);
			break;
		case jbvNumeric:
			Assert(!is_key);

			/*
			 * A normalized textual representation, free of trailing zeroes,
			 * is required so that numerically equal values will produce equal
			 * strings.
			 *
			 * It isn't ideal that numerics are stored in a relatively bulky
			 * textual format.  However, it's a notationally convenient way of
			 * storing a "union" type in the GIN B-Tree, and indexing Jsonb
			 * strings takes precedence.
			 */
			cstr = numeric_normalize(scalarVal->val.numeric);
			item = make_text_key(JGINFLAG_NUM, cstr, strlen(cstr));
			pfree(cstr);
			break;
		case jbvString:
			item = make_text_key(is_key ? JGINFLAG_KEY : JGINFLAG_STR,
								 scalarVal->val.string.val,
								 scalarVal->val.string.len);
			break;
		default:
			elog(ERROR, "unrecognized jsonb scalar type: %d", scalarVal->type);
			item = 0;			/* keep compiler quiet */
			break;
	}

	return item;
}
