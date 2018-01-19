/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.c
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "executor/execExpr.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/json.h"
#include "utils/jsonpath.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/varlena.h"

#ifdef JSONPATH_JSON_C
#define JSONXOID JSONOID
#else
#define JSONXOID JSONBOID
#endif

typedef struct JsonTableScanState JsonTableScanState;
typedef struct JsonTableJoinState JsonTableJoinState;

struct JsonTableScanState
{
	JsonTableScanState *parent;
	JsonTableJoinState *nested;
	MemoryContext mcxt;
	JsonPath   *path;
	List	   *args;
	JsonValueList found;
	JsonValueListIterator iter;
	Datum		current;
	int			ordinal;
	bool		outerJoin;
	bool		errorOnError;
	bool		advanceNested;
	bool		reset;
};

struct JsonTableJoinState
{
	union
	{
		struct
		{
			JsonTableJoinState *left;
			JsonTableJoinState *right;
			bool		cross;
			bool		advanceRight;
		}			join;
		JsonTableScanState scan;
	}			u;
	bool		is_join;
};

/* random number to identify JsonTableContext */
#define JSON_TABLE_CONTEXT_MAGIC	418352867

typedef struct JsonTableContext
{
	int			magic;
	struct
	{
		ExprState  *expr;
		JsonTableScanState *scan;
	}		   *colexprs;
	JsonTableScanState root;
	bool		empty;
} JsonTableContext;

static inline JsonPathExecResult recursiveExecute(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonbValue *jb,
										   JsonValueList *found);

static inline JsonPathExecResult recursiveExecuteBool(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonbValue *jb);

static inline JsonPathExecResult recursiveExecuteNested(JsonPathExecContext *cxt,
											JsonPathItem *jsp, JsonbValue *jb,
											JsonValueList *found);

static inline JsonPathExecResult recursiveExecuteUnwrap(JsonPathExecContext *cxt,
							JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);

static inline JsonbValue *wrapItem(JsonbValue *jbv);

static inline JsonbValue *wrapItemsInArray(const JsonValueList *items);


static JsonTableJoinState *JsonTableInitPlanState(JsonTableContext *cxt,
									Node *plan, JsonTableScanState *parent);

static bool JsonTableNextRow(JsonTableScanState *scan);

static Datum returnDATUM(void *arg, bool *isNull);

void
JsonValueListConcat(JsonValueList *jvl1, JsonValueList jvl2)
{
	if (jvl1->singleton)
	{
		if (jvl2.singleton)
			jvl1->list = list_make2(jvl1->singleton, jvl2.singleton);
		else
			jvl1->list = lcons(jvl1->singleton, jvl2.list);

		jvl1->singleton = NULL;
	}
	else if (jvl2.singleton)
	{
		if (jvl1->list)
			jvl1->list = lappend(jvl1->list, jvl2.singleton);
		else
			jvl1->singleton = jvl2.singleton;
	}
	else if (jvl1->list)
		jvl1->list = list_concat(jvl1->list, jvl2.list);
	else
		jvl1->list = jvl2.list;
}

static inline void
JsonValueListClear(JsonValueList *jvl)
{
	jvl->singleton = NULL;
	jvl->list = NIL;
}

#ifndef JSONPATH_JSON_C
/*
 * Initialize a binary JsonbValue with the given jsonb container.
 */
static inline JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = &jb->root;
	jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

	return jbv;
}
#endif

/*
 * Transform a JsonbValue into a binary JsonbValue by encoding it to a
 * binary jsonb container.
 */
static inline JsonbValue *
JsonbWrapInBinary(JsonbValue *jbv, JsonbValue *out)
{
	Jsonb	   *jb = JsonbValueToJsonb(jbv);

	if (!out)
		out = palloc(sizeof(*out));

	return JsonbInitBinary(out, jb);
}


/********************Execute functions for JsonPath***************************/

/*
 * Find value of jsonpath variable in a list of passing params
 */
static void
computeJsonPathVariable(JsonPathItem *variable, List *vars, JsonbValue *value)
{
	ListCell			*cell;
	JsonPathVariable	*var = NULL;
	bool				isNull;
	Datum				computedValue;
	char				*varName;
	int					varNameLength;

	Assert(variable->type == jpiVariable);
	varName = jspGetString(variable, &varNameLength);

	foreach(cell, vars)
	{
		var = (JsonPathVariable*)lfirst(cell);

		if (varNameLength == VARSIZE_ANY_EXHDR(var->varName) &&
			!strncmp(varName, VARDATA_ANY(var->varName), varNameLength))
			break;

		var = NULL;
	}

	if (var == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("could not find '%s' passed variable",
						pnstrdup(varName, varNameLength))));

	computedValue = var->cb(var->cb_arg, &isNull);

	if (isNull)
	{
		value->type = jbvNull;
		return;
	}

	switch(var->typid)
	{
		case BOOLOID:
			value->type = jbvBool;
			value->val.boolean = DatumGetBool(computedValue);
			break;
		case NUMERICOID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(computedValue);
			break;
			break;
		case INT2OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												int2_numeric, computedValue));
			break;
		case INT4OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												int4_numeric, computedValue));
			break;
		case INT8OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												int8_numeric, computedValue));
			break;
		case FLOAT4OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												float4_numeric, computedValue));
			break;
		case FLOAT8OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												float4_numeric, computedValue));
			break;
		case TEXTOID:
		case VARCHAROID:
			value->type = jbvString;
			value->val.string.val = VARDATA_ANY(computedValue);
			value->val.string.len = VARSIZE_ANY_EXHDR(computedValue);
			break;
		case DATEOID:
		case TIMEOID:
		case TIMETZOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			value->type = jbvDatetime;
			value->val.datetime.typid = var->typid;
			value->val.datetime.typmod = var->typmod;
			value->val.datetime.value = computedValue;
			break;
		case JSONXOID:
			{
				Jsonb	   *jb = DatumGetJsonbP(computedValue);

				if (JB_ROOT_IS_SCALAR(jb))
					JsonbExtractScalar(&jb->root, value);
				else
					JsonbInitBinary(value, jb);
			}
			break;
		case (Oid) -1: /* raw JsonbValue */
			*value = *(JsonbValue *) DatumGetPointer(computedValue);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("only bool, numeric and text types could be casted to supported jsonpath types")));
	}
}

/*
 * Convert jsonpath's scalar or variable node to actual jsonb value
 */
static void
computeJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item, JsonbValue *value)
{
	switch(item->type)
	{
		case jpiNull:
			value->type = jbvNull;
			break;
		case jpiBool:
			value->type = jbvBool;
			value->val.boolean = jspGetBool(item);
			break;
		case jpiNumeric:
			value->type = jbvNumeric;
			value->val.numeric = jspGetNumeric(item);
			break;
		case jpiString:
			value->type = jbvString;
			value->val.string.val = jspGetString(item, &value->val.string.len);
			break;
		case jpiVariable:
			computeJsonPathVariable(item, cxt->vars, value);
			break;
		case jpiArgument:
			{
				JsonLambdaArg *arg;
				char	   *argname;
				int			argnamelen;

				argname = jspGetString(item, &argnamelen);

				for (arg = cxt->args; arg; arg = arg->next)
				{
					if (arg->namelen == argnamelen &&
						!strncmp(arg->name, argname, argnamelen))
					{
						*value = *arg->val;
						return;
					}
				}

				ereport(ERROR,
						(errcode(ERRCODE_NO_DATA_FOUND),
						 errmsg("could not find '%s' lambda variable",
								pnstrdup(argname, argnamelen))));
			}
			break;
		default:
			elog(ERROR, "Wrong type");
	}
}

/*
 * Get the type name of a SQL/JSON item.
 */
static const char *
JsonbTypeName(JsonbValue *jb)
{
	JsonbValue jbvbuf;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = (void *) jb->val.binary.data;

		if (JsonContainerIsScalar(jbc))
			jb = JsonbExtractScalar(jbc, &jbvbuf);
		else if (JsonContainerIsArray(jbc))
			return "array";
		else if (JsonContainerIsObject(jbc))
			return "object";
		else
			elog(ERROR, "Unknown container type: 0x%08x", jbc->header);
	}

	switch (jb->type)
	{
		case jbvObject:
			return "object";
		case jbvArray:
			return "array";
		case jbvNumeric:
			return "number";
		case jbvString:
			return "string";
		case jbvBool:
			return "boolean";
		case jbvNull:
			return "null";
		case jbvDatetime:
			switch (jb->val.datetime.typid)
			{
				case DATEOID:
					return "date";
				case TIMEOID:
					return "time without time zone";
				case TIMETZOID:
					return "time with time zone";
				case TIMESTAMPOID:
					return "timestamp without time zone";
				case TIMESTAMPTZOID:
					return "timestamp with time zone";
				default:
					elog(ERROR, "unknown jsonb value datetime type oid %d",
						 jb->val.datetime.typid);
			}
			return "unknown";
		default:
			elog(ERROR, "Unknown jsonb value type: %d", jb->type);
			return "unknown";
	}
}

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
int
JsonbArraySize(JsonbValue *jb)
{
	if (jb->type == jbvArray)
		return jb->val.array.nElems;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc =  (void *) jb->val.binary.data;

		if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
			return JsonContainerSize(jbc);
	}

	return -1;
}

/*
 * Compare two numerics.
 */
static int
compareNumeric(Numeric a, Numeric b)
{
	return	DatumGetInt32(
				DirectFunctionCall2(
					numeric_cmp,
					PointerGetDatum(a),
					PointerGetDatum(b)
				)
			);
}

/*
 * Cross-type comparison of two datetime SQL/JSON items.  If items are
 * uncomparable, 'error' flag is set.
 */
static int
compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2, bool *error)
{
	PGFunction	cmpfunc = NULL;

	switch (typid1)
	{
		case DATEOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = date_cmp;
					break;
				case TIMESTAMPOID:
					cmpfunc = date_cmp_timestamp;
					break;
				case TIMESTAMPTZOID:
					cmpfunc = date_cmp_timestamptz;
					break;
				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMEOID:
			switch (typid2)
			{
				case TIMEOID:
					cmpfunc = time_cmp;
					break;
				case TIMETZOID:
					val1 = DirectFunctionCall1(time_timetz, val1);
					cmpfunc = timetz_cmp;
					break;
				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMETZOID:
			switch (typid2)
			{
				case TIMEOID:
					val2 = DirectFunctionCall1(time_timetz, val2);
					cmpfunc = timetz_cmp;
					break;
				case TIMETZOID:
					cmpfunc = timetz_cmp;
					break;
				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMESTAMPOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = timestamp_cmp_date;
					break;
				case TIMESTAMPOID:
					cmpfunc = timestamp_cmp;
					break;
				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp_timestamptz;
					break;
				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMESTAMPTZOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = timestamptz_cmp_date;
					break;
				case TIMESTAMPOID:
					cmpfunc = timestamptz_cmp_timestamp;
					break;
				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp;
					break;
				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		default:
			elog(ERROR, "unknown SQL/JSON datetime type oid: %d", typid1);
	}

	if (!cmpfunc)
		elog(ERROR, "unknown SQL/JSON datetime type oid: %d", typid2);

	*error = false;

	return DatumGetInt32(DirectFunctionCall2(cmpfunc, val1, val2));
}

/*
 * Check equality of two SLQ/JSON items of the same type.
 */
static inline JsonPathExecResult
checkEquality(JsonbValue *jb1, JsonbValue *jb2, bool not)
{
	bool	eq = false;

	if (jb1->type != jb2->type)
	{
		if (jb1->type == jbvNull || jb2->type == jbvNull)
			return not ? jperOk : jperNotFound;

		return jperError;
	}

	switch (jb1->type)
	{
		case jbvNull:
			eq = true;
			break;
		case jbvString:
			eq = (jb1->val.string.len == jb2->val.string.len &&
					memcmp(jb2->val.string.val, jb1->val.string.val,
						   jb1->val.string.len) == 0);
			break;
		case jbvBool:
			eq = (jb2->val.boolean == jb1->val.boolean);
			break;
		case jbvNumeric:
			eq = (compareNumeric(jb1->val.numeric, jb2->val.numeric) == 0);
			break;
		case jbvDatetime:
			{
				bool		error;

				eq = compareDatetime(jb1->val.datetime.value,
									 jb1->val.datetime.typid,
									 jb2->val.datetime.value,
									 jb2->val.datetime.typid,
									 &error) == 0;

				if (error)
					return jperError;

				break;
			}

		case jbvBinary:
		case jbvObject:
		case jbvArray:
			return jperError;

		default:
			elog(ERROR, "Unknown jsonb value type %d", jb1->type);
	}

	return (not ^ eq) ? jperOk : jperNotFound;
}

/*
 * Compare two SLQ/JSON items using comparison operation 'op'.
 */
JsonPathExecResult
jspCompareItems(int32 op, JsonbValue *jb1, JsonbValue *jb2)
{
	int			cmp;
	bool		res;

	if (jb1->type != jb2->type)
	{
		if (jb1->type != jbvNull && jb2->type != jbvNull)
			/* non-null items of different types are not order-comparable */
			return jperError;

		if (jb1->type != jbvNull || jb2->type != jbvNull)
			/* comparison of nulls to non-nulls returns always false */
			return jperNotFound;

		/* both values are JSON nulls */
	}

	switch (jb1->type)
	{
		case jbvNull:
			cmp = 0;
			break;
		case jbvNumeric:
			cmp = compareNumeric(jb1->val.numeric, jb2->val.numeric);
			break;
		case jbvString:
			cmp = varstr_cmp(jb1->val.string.val, jb1->val.string.len,
							 jb2->val.string.val, jb2->val.string.len,
							 DEFAULT_COLLATION_OID);
			break;
		case jbvDatetime:
			{
				bool		error;

				cmp = compareDatetime(jb1->val.datetime.value,
									  jb1->val.datetime.typid,
									  jb2->val.datetime.value,
									  jb2->val.datetime.typid,
									  &error);

				if (error)
					return jperError;
			}
			break;
		default:
			return jperError;
	}

	switch (op)
	{
		case jpiEqual:
			res = (cmp == 0);
			break;
		case jpiNotEqual:
			res = (cmp != 0);
			break;
		case jpiLess:
			res = (cmp < 0);
			break;
		case jpiGreater:
			res = (cmp > 0);
			break;
		case jpiLessOrEqual:
			res = (cmp <= 0);
			break;
		case jpiGreaterOrEqual:
			res = (cmp >= 0);
			break;
		default:
			elog(ERROR, "Unknown operation");
			return jperError;
	}

	return res ? jperOk : jperNotFound;
}

/*
 * Execute next jsonpath item if it does exist.
 */
static inline JsonPathExecResult
recursiveExecuteNext(JsonPathExecContext *cxt,
					 JsonPathItem *cur, JsonPathItem *next,
					 JsonbValue *v, JsonValueList *found, bool copy)
{
	JsonPathItem elem;
	bool		hasNext;

	if (!cur)
		hasNext = next != NULL;
	else if (next)
		hasNext = jspHasNext(cur);
	else
	{
		next = &elem;
		hasNext = jspGetNext(cur, next);
	}

	if (hasNext)
		return recursiveExecute(cxt, next, v, found);

	if (found)
		JsonValueListAppend(found, copy ? copyJsonbValue(v) : v);

	return jperOk;
}

/*
 * Execute jsonpath expression and automatically unwrap each array item from
 * the resulting sequence in lax mode.
 */
static inline JsonPathExecResult
recursiveExecuteAndUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb, JsonValueList *found)
{
	if (cxt->lax)
	{
		JsonValueList seq = { 0 };
		JsonValueListIterator it = { 0 };
		JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &seq);
		JsonbValue *item;

		if (jperIsError(res))
			return res;

		while ((item = JsonValueListNext(&seq, &it)))
		{
			if (item->type == jbvArray)
			{
				JsonbValue *elem = item->val.array.elems;
				JsonbValue *last = elem + item->val.array.nElems;

				for (; elem < last; elem++)
					JsonValueListAppend(found, copyJsonbValue(elem));
			}
			else if (item->type == jbvBinary &&
					 JsonContainerIsArray(item->val.binary.data))
			{
				JsonbValue	elem;
				JsonbIterator *it = JsonbIteratorInit(item->val.binary.data);
				JsonbIteratorToken tok;

				while ((tok = JsonbIteratorNext(&it, &elem, true)) != WJB_DONE)
				{
					if (tok == WJB_ELEM)
						JsonValueListAppend(found, copyJsonbValue(&elem));
				}
			}
			else
				JsonValueListAppend(found, item);
		}

		return jperOk;
	}

	return recursiveExecute(cxt, jsp, jb, found);
}

/*
 * Execute comparison expression.  True is returned only if found any pair of
 * items from the left and right operand's sequences which is satifistfying
 * condition.  In strict mode all pairs should be comparable, otherwise an error
 * is returned.
 */
static JsonPathExecResult
executeExpr(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	JsonValueList lseq = { 0 };
	JsonValueList rseq = { 0 };
	JsonValueListIterator lseqit = { 0 };
	JsonbValue *lval;
	bool		error = false;
	bool		found = false;

	jspGetLeftArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperError;

	jspGetRightArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &rseq);
	if (jperIsError(res))
		return jperError;

	while ((lval = JsonValueListNext(&lseq, &lseqit)))
	{
		JsonValueListIterator rseqit = { 0 };
		JsonbValue *rval;

		while ((rval = JsonValueListNext(&rseq, &rseqit)))
		{
			switch (jsp->type)
			{
				case jpiEqual:
					res = checkEquality(lval, rval, false);
					break;
				case jpiNotEqual:
					res = checkEquality(lval, rval, true);
					break;
				case jpiLess:
				case jpiGreater:
				case jpiLessOrEqual:
				case jpiGreaterOrEqual:
					res = jspCompareItems(jsp->type, lval, rval);
					break;
				default:
					elog(ERROR, "Unknown operation");
			}

			if (res == jperOk)
			{
				if (cxt->lax)
					return jperOk;

				found = true;
			}
			else if (res == jperError)
			{
				if (!cxt->lax)
					return jperError;

				error = true;
			}
		}
	}

	if (found) /* possible only in strict mode */
		return jperOk;

	if (error) /* possible only in lax mode */
		return jperError;

	return jperNotFound;
}

/*
 * Execute binary arithemitc expression on singleton numeric operands.
 * Array operands are automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeBinaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
						JsonbValue *jb, JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathItem elem;
	JsonValueList lseq = { 0 };
	JsonValueList rseq = { 0 };
	JsonbValue *lval;
	JsonbValue *rval;
	JsonbValue	lvalbuf;
	JsonbValue	rvalbuf;
	Datum		ldatum;
	Datum		rdatum;
	Datum		res;
	bool		hasNext;

	jspGetLeftArg(jsp, &elem);

	/* XXX by standard unwrapped only operands of multiplicative expressions */
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);

	if (jper == jperOk)
	{
		jspGetRightArg(jsp, &elem);
		jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &rseq); /* XXX */
	}

	if (jper != jperOk ||
		JsonValueListLength(&lseq) != 1 ||
		JsonValueListLength(&rseq) != 1)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	lval = JsonValueListHead(&lseq);

	if (JsonbType(lval) == jbvScalar)
		lval = JsonbExtractScalar(lval->val.binary.data, &lvalbuf);

	if (lval->type != jbvNumeric)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	rval = JsonValueListHead(&rseq);

	if (JsonbType(rval) == jbvScalar)
		rval = JsonbExtractScalar(rval->val.binary.data, &rvalbuf);

	if (rval->type != jbvNumeric)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	hasNext = jspGetNext(jsp, &elem);

	if (!found && !hasNext)
		return jperOk;

	ldatum = NumericGetDatum(lval->val.numeric);
	rdatum = NumericGetDatum(rval->val.numeric);

	switch (jsp->type)
	{
		case jpiAdd:
			res = DirectFunctionCall2(numeric_add, ldatum, rdatum);
			break;
		case jpiSub:
			res = DirectFunctionCall2(numeric_sub, ldatum, rdatum);
			break;
		case jpiMul:
			res = DirectFunctionCall2(numeric_mul, ldatum, rdatum);
			break;
		case jpiDiv:
			res = DirectFunctionCall2(numeric_div, ldatum, rdatum);
			break;
		case jpiMod:
			res = DirectFunctionCall2(numeric_mod, ldatum, rdatum);
			break;
		default:
			elog(ERROR, "unknown jsonpath arithmetic operation %d", jsp->type);
	}

	lval = palloc(sizeof(*lval));
	lval->type = jbvNumeric;
	lval->val.numeric = DatumGetNumeric(res);

	return recursiveExecuteNext(cxt, jsp, &elem, lval, found, false);
}

/*
 * Execute unary arithemitc expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb,  JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathExecResult jper2;
	JsonPathItem elem;
	JsonValueList seq = { 0 };
	JsonValueListIterator it = { 0 };
	JsonbValue *val;
	bool		hasNext;

	jspGetArg(jsp, &elem);
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &seq);

	if (jperIsError(jper))
		return jperMakeError(ERRCODE_JSON_NUMBER_NOT_FOUND);

	jper = jperNotFound;

	hasNext = jspGetNext(jsp, &elem);

	while ((val = JsonValueListNext(&seq, &it)))
	{
		if (JsonbType(val) == jbvScalar)
			JsonbExtractScalar(val->val.binary.data, val);

		if (val->type == jbvNumeric)
		{
			if (!found && !hasNext)
				return jperOk;
		}
		else if (!found && !hasNext)
			continue; /* skip non-numerics processing */

		if (val->type != jbvNumeric)
			return jperMakeError(ERRCODE_JSON_NUMBER_NOT_FOUND);

		switch (jsp->type)
		{
			case jpiPlus:
				break;
			case jpiMinus:
				val->val.numeric =
					DatumGetNumeric(DirectFunctionCall1(
						numeric_uminus, NumericGetDatum(val->val.numeric)));
				break;
			default:
				elog(ERROR, "unknown jsonpath arithmetic operation %d", jsp->type);
		}

		jper2 = recursiveExecuteNext(cxt, jsp, &elem, val, found, false);

		if (jperIsError(jper2))
			return jper2;

		if (jper2 == jperOk)
		{
			if (!found)
				return jperOk;
			jper = jperOk;
		}
	}

	return jper;
}

static JsonValueList
prependKey(JsonbValue *key, const JsonValueList *items)
{
	JsonValueList objs = { 0 };
	JsonValueListIterator it = { 0 };
	JsonbValue *val;

	while ((val = JsonValueListNext(items, &it)))
	{
		JsonbValue *obj;
		JsonbValue	bin;
		JsonbParseState *ps = NULL;

		if (val->type == jbvObject || val->type == jbvArray)
			val = JsonbWrapInBinary(val, &bin);

		pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);
		pushJsonbValue(&ps, WJB_KEY, key);
		pushJsonbValue(&ps, WJB_VALUE, val);
		obj = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

		JsonValueListAppend(&objs, obj);
	}

	return objs;
}

/*
 * implements jpiAny node (** operator)
 */
static JsonPathExecResult
recursiveAny(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			 JsonValueList *found, bool outPath,
			 uint32 level, uint32 first, uint32 last)
{
	JsonPathExecResult	res = jperNotFound;
	JsonbIterator		*it;
	int32				r;
	JsonbValue			v;
	bool				isObject;
	JsonValueList		items = { 0 };
	JsonValueList	   *pitems = found;

	check_stack_depth();

	if (level > last)
		return res;

	if (pitems && outPath)
		pitems = &items;

	isObject = JsonContainerIsObject(jb->val.binary.data);

	it = JsonbIteratorInit(jb->val.binary.data);

	/*
	 * Recursivly iterate over jsonb objects/arrays
	 */
	while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		JsonbValue	key;

		if (r == WJB_KEY)
		{
			key = v;
			r = JsonbIteratorNext(&it, &v, true);
			Assert(r == WJB_VALUE);

			if (pitems == &items)
				JsonValueListClear(pitems);
		}

		if (r == WJB_VALUE || r == WJB_ELEM)
		{
			if (level >= first)
			{
				/* check expression */
				res = recursiveExecuteNext(cxt, NULL, jsp, &v, pitems, true);

				if (jperIsError(res))
					break;

				if (res == jperOk && !found)
					break;
			}

			if (level < last && v.type == jbvBinary)
			{
				res = recursiveAny(cxt, jsp, &v, pitems, outPath,
								   level + 1, first, last);

				if (jperIsError(res))
					break;

				if (res == jperOk && found == NULL)
					break;
			}

			if (isObject && !JsonValueListIsEmpty(&items) && !jperIsError(res))
				JsonValueListConcat(found, prependKey(&key, &items));
		}
	}

	if (!isObject && !JsonValueListIsEmpty(&items) && !jperIsError(res))
		JsonValueListAppend(found, wrapItemsInArray(&items));

	return res;
}

/*
 * Execute array subscript expression and convert resulting numeric item to the
 * integer type with truncation.
 */
static JsonPathExecResult
getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			  int32 *index)
{
	JsonbValue *jbv;
	JsonValueList found = { 0 };
	JsonbValue	tmp;
	JsonPathExecResult res = recursiveExecuteNested(cxt, jsp, jb, &found);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&found) != 1)
		return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

	jbv = JsonValueListHead(&found);

	if (JsonbType(jbv) == jbvScalar)
		jbv = JsonbExtractScalar(jbv->val.binary.data, &tmp);

	if (jbv->type != jbvNumeric)
		return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

	*index = DatumGetInt32(DirectFunctionCall1(numeric_int4,
							DirectFunctionCall2(numeric_trunc,
											NumericGetDatum(jbv->val.numeric),
											Int32GetDatum(0))));

	return jperOk;
}

static JsonPathExecResult
executeStartsWithPredicate(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	JsonValueList lseq = { 0 };
	JsonValueList rseq = { 0 };
	JsonValueListIterator lit = { 0 };
	JsonbValue *whole;
	JsonbValue *initial;
	JsonbValue	initialbuf;
	bool		error = false;
	bool		found = false;

	jspGetRightArg(jsp, &elem);
	res = recursiveExecute(cxt, &elem, jb, &rseq);
	if (jperIsError(res))
		return jperError;

	if (JsonValueListLength(&rseq) != 1)
		return jperError;

	initial = JsonValueListHead(&rseq);

	if (JsonbType(initial) == jbvScalar)
		initial = JsonbExtractScalar(initial->val.binary.data, &initialbuf);

	if (initial->type != jbvString)
		return jperError;

	jspGetLeftArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperError;

	while ((whole = JsonValueListNext(&lseq, &lit)))
	{
		JsonbValue	wholebuf;

		if (JsonbType(whole) == jbvScalar)
			whole = JsonbExtractScalar(whole->val.binary.data, &wholebuf);

		if (whole->type != jbvString)
		{
			if (!cxt->lax)
				return jperError;

			error = true;
		}
		else if (whole->val.string.len >= initial->val.string.len &&
				 !memcmp(whole->val.string.val,
						 initial->val.string.val,
						 initial->val.string.len))
		{
			if (cxt->lax)
				return jperOk;

			found = true;
		}
	}

	if (found) /* possible only in strict mode */
		return jperOk;

	if (error) /* possible only in lax mode */
		return jperError;

	return jperNotFound;
}

static JsonPathExecResult
executeLikeRegexPredicate(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	JsonValueList seq = { 0 };
	JsonValueListIterator it = { 0 };
	JsonbValue *str;
	text	   *regex;
	uint32		flags = jsp->content.like_regex.flags;
	int			cflags = REG_ADVANCED;
	bool		error = false;
	bool		found = false;

	if (flags & JSP_REGEX_ICASE)
		cflags |= REG_ICASE;
	if (flags & JSP_REGEX_MLINE)
		cflags |= REG_NEWLINE;
	if (flags & JSP_REGEX_SLINE)
		cflags &= ~REG_NEWLINE;
	if (flags & JSP_REGEX_WSPACE)
		cflags |= REG_EXPANDED;

	regex = cstring_to_text_with_len(jsp->content.like_regex.pattern,
									 jsp->content.like_regex.patternlen);

	jspInitByBuffer(&elem, jsp->base, jsp->content.like_regex.expr);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &seq);
	if (jperIsError(res))
		return jperError;

	while ((str = JsonValueListNext(&seq, &it)))
	{
		JsonbValue	strbuf;

		if (JsonbType(str) == jbvScalar)
			str = JsonbExtractScalar(str->val.binary.data, &strbuf);

		if (str->type != jbvString)
		{
			if (!cxt->lax)
				return jperError;

			error = true;
		}
		else if (RE_compile_and_execute(regex, str->val.string.val,
										str->val.string.len, cflags,
										DEFAULT_COLLATION_OID, 0, NULL))
		{
			if (cxt->lax)
				return jperOk;

			found = true;
		}
	}

	if (found) /* possible only in strict mode */
		return jperOk;

	if (error) /* possible only in lax mode */
		return jperError;

	return jperNotFound;
}

/*
 * Try to parse datetime text with the specified datetime template.
 * Returns 'value' datum, its type 'typid' and 'typmod'.
 */
static bool
tryToParseDatetime(const char *template, text *datetime,
				   Datum *value, Oid *typid, int32 *typmod)
{
	MemoryContext mcxt = CurrentMemoryContext;
	bool		ok = false;

	PG_TRY();
	{
		*value = to_datetime(datetime, template, -1, true, typid, typmod);
		ok = true;
	}
	PG_CATCH();
	{
		if (ERRCODE_TO_CATEGORY(geterrcode()) != ERRCODE_DATA_EXCEPTION)
			PG_RE_THROW();

		FlushErrorState();
		MemoryContextSwitchTo(mcxt);
	}
	PG_END_TRY();

	return ok;
}

typedef struct JsonPathFuncCache
{
	FmgrInfo	finfo;
	JsonPathItem *args;
	void	  **argscache;
} JsonPathFuncCache;

static JsonPathFuncCache *
prepareFunctionCache(JsonPathExecContext *cxt, JsonPathItem *jsp)
{
	MemoryContext oldcontext;
	JsonPathFuncCache *cache = cxt->cache[jsp->content.func.id];
	List	   *funcname = list_make1(makeString(jsp->content.func.name));
	Oid			argtypes[] = { JSONPATH_FCXTOID, JSONXOID };
	Oid			funcid;
	int32		i;

	if (cache)
		return cache;

	funcid = LookupFuncName(funcname,
							sizeof(argtypes) / sizeof(argtypes[0]), argtypes,
							false);

	if (get_func_rettype(funcid) != INT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("return type of jsonpath item function %s is not %s",
						 NameListToString(funcname), format_type_be(INT8OID))));

	oldcontext = MemoryContextSwitchTo(cxt->cache_mcxt);

	cache = cxt->cache[jsp->content.func.id] = palloc0(sizeof(*cache));

	fmgr_info(funcid, &cache->finfo);

	cache->args = palloc(sizeof(*cache->args) * jsp->content.func.nargs);
	cache->argscache = palloc0(sizeof(*cache->argscache) *
							   jsp->content.func.nargs);

	for (i = 0; i < jsp->content.func.nargs; i++)
		jspGetFunctionArg(jsp, i, &cache->args[i]);

	MemoryContextSwitchTo(oldcontext);

	return cache;
}

static JsonPathExecResult
executeFunction(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
				JsonValueList *result, bool needBool)
{
	JsonPathFuncCache *cache = prepareFunctionCache(cxt, jsp);
	JsonPathFuncContext fcxt;
	JsonValueList tmpres = { 0 };
	JsonValueListIterator tmpiter = { 0 };
	JsonPathExecResult res;
	JsonbValue *jbvitem;

	fcxt.cxt = cxt;
	fcxt.funcname = jsp->content.func.name;
	fcxt.jb = jb;
	fcxt.result = jspHasNext(jsp) ? &tmpres : result;
	fcxt.args = cache->args;
	fcxt.argscache = cache->argscache;
	fcxt.nargs = jsp->content.func.nargs;

	if (jsp->type == jpiMethod)
	{
		JsonValueList items = { 0 };
		JsonValueListIterator iter = { 0 };

		/* skip first item argument */
		fcxt.args++;
		fcxt.argscache++;
		fcxt.nargs--;

		res = recursiveExecute(cxt, &cache->args[0], jb, &items);

		if (jperIsError(res))
			return res;

		while ((jbvitem = JsonValueListNext(&items, &iter)))
		{
			fcxt.item = jbvitem;

			res = DatumGetInt64(FunctionCall2(&cache->finfo,
											  PointerGetDatum(&fcxt),
											  PointerGetDatum(NULL)));
			if (jperIsError(res))
				return res;
		}
	}
	else
	{
		fcxt.item = NULL;

		res = DatumGetInt64(FunctionCall2(&cache->finfo,
										  PointerGetDatum(&fcxt),
										  PointerGetDatum(NULL)));
		if (jperIsError(res))
			return res;
	}

	if (!jspHasNext(jsp))
		return res;

	while ((jbvitem = JsonValueListNext(&tmpres, &tmpiter)))
	{
		res = recursiveExecuteNext(cxt, jsp, NULL, jbvitem, result, needBool);

		if (jperIsError(res))
			return res;
	}

	return res;
}

/*
 * Convert execution status 'res' to a boolean JSON item and execute next
 * jsonpath if 'needBool' is false:
 *  - jperOk => true
 *  - jperNotFound => false
 *  - jperError => null (errors are converted to NULL values)
 */
static inline JsonPathExecResult
appendBoolResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
				 JsonValueList *found, JsonPathExecResult res, bool needBool)
{
	JsonPathItem next;
	JsonbValue	jbv;
	bool		hasNext = jspGetNext(jsp, &next);

	if (needBool)
	{
		Assert(!hasNext);
		return res;	/* simply return status */
	}

	if (!found && !hasNext)
		return jperOk;	/* found singleton boolean value */

	if (jperIsError(res))
		jbv.type = jbvNull;
	else
	{
		jbv.type = jbvBool;
		jbv.val.boolean = res == jperOk;
	}

	return recursiveExecuteNext(cxt, jsp, &next, &jbv, found, true);
}

static inline JsonPathExecResult
recursiveExecuteNested(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, JsonValueList *found)
{
	JsonItemStackEntry current;
	JsonPathExecResult res;

	pushJsonItem(&cxt->stack, &current, jb);

	/* found == NULL is used here when executing boolean filter expressions */
	res = found
		? recursiveExecute(cxt, jsp, jb, found)
		: recursiveExecuteBool(cxt, jsp, jb);

	popJsonItem(&cxt->stack);

	return res;
}

JsonPathExecResult
jspRecursiveExecuteNested(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb, JsonValueList *found)
{
	return recursiveExecuteNested(cxt, jsp, jb, found);
}

typedef union JsonLambdaCache
{
	JsonLambdaArg *args;
	JsonPathVariable *vars;
} JsonLambdaCache;

static JsonPathExecResult
recursiveExecuteLambdaVars(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb, JsonValueList *found,
						   JsonbValue **params, int nparams, void **pcache)
{
	JsonLambdaCache *cache = *pcache;
	JsonPathExecResult res;
	int			i;

	if (nparams > 0 && !cache)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(cxt->cache_mcxt);

		cache = *pcache = palloc0(sizeof(*cache));

		cache->vars = palloc(sizeof(*cache->vars) * nparams);

		for (i = 0; i < nparams; i++)
		{
			JsonPathVariable *var = &cache->vars[i];
			char		varname[20];

			snprintf(varname, sizeof(varname), "%d", i + 1);

			var->cb = returnDATUM;
			var->varName = cstring_to_text(varname);
			var->typid = (Oid) -1; /* raw JsonbValue */
			var->typmod = -1;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	for (i = 0; i < nparams; i++)
	{
		cache->vars[i].cb_arg = params[i];
		cxt->vars = lcons(&cache->vars[i], cxt->vars);
	}

	res = found
		? recursiveExecute(cxt, jsp, jb, found)
		: recursiveExecuteBool(cxt, jsp, jb);

	for (i = 0; i < nparams; i++)
		cxt->vars = list_delete_first(cxt->vars);

	return res;
}

static inline JsonPathExecResult
recursiveExecuteLambdaExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb, JsonValueList *found,
						   JsonbValue **params, int nparams, void **pcache)
{
	JsonPathItem expr;
	JsonPathExecResult res;
	JsonLambdaCache *cache = *pcache;
	JsonLambdaArg *oldargs;
	int			i;

	if (jsp->content.lambda.nparams > nparams)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jsonpath lambda arguments mismatch: expected %d but given %d",
						jsp->content.lambda.nparams, nparams)));

	if (jsp->content.lambda.nparams > 0 && !cache)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(cxt->cache_mcxt);

		cache = *pcache = palloc0(sizeof(*cache));
		cache->args = palloc(sizeof(cache->args[0]) *
							 jsp->content.lambda.nparams);

		for (i = 0; i < jsp->content.lambda.nparams; i++)
		{
			JsonLambdaArg *arg = &cache->args[i];
			JsonPathItem argname;

			jspGetLambdaParam(jsp, i, &argname);

			if (argname.type != jpiArgument)
				elog(ERROR, "invalid jsonpath lambda argument item type: %d",
					 argname.type);

			arg->name = jspGetString(&argname, &arg->namelen);
			arg->val = NULL;
			arg->next = arg + 1;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	oldargs = cxt->args;

	if (jsp->content.lambda.nparams > 0)
	{
		for (i = 0; i < jsp->content.lambda.nparams; i++)
			cache->args[i].val = params[i];

		cache->args[jsp->content.lambda.nparams - 1].next = oldargs;
		cxt->args = &cache->args[0];
	}

	jspGetLambdaExpr(jsp, &expr);

	/* found == NULL is used here when executing boolean filter expressions */
	res = found
		? recursiveExecute(cxt, &expr, jb, found)
		: recursiveExecuteBool(cxt, &expr, jb);

	cxt->args = oldargs;

	return res;
}

JsonPathExecResult
jspRecursiveExecuteLambda(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb, JsonValueList *res,
						  JsonbValue **params, int nparams, void **cache)
{
	return jsp->type == jpiLambda
		? recursiveExecuteLambdaExpr(cxt, jsp, jb, res, params, nparams, cache)
		: recursiveExecuteLambdaVars(cxt, jsp, jb, res, params, nparams, cache);
}

/*
 * Main executor function: walks on jsonpath structure and tries to find
 * correspoding parts of jsonb. Note, jsonb and jsonpath values should be
 * avaliable and untoasted during work because JsonPathItem, JsonbValue
 * and found could have pointers into input values. If caller wants just to
 * check matching of json by jsonpath then it doesn't provide a found arg.
 * In this case executor works till first positive result and does not check
 * the rest if it is possible. In other case it tries to find all satisfied
 * results
 */
static JsonPathExecResult
recursiveExecuteNoUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
						 JsonbValue *jb, JsonValueList *found, bool needBool)
{
	JsonPathItem		elem;
	JsonPathExecResult	res = jperNotFound;
	bool				hasNext;

	check_stack_depth();

	switch(jsp->type) {
		case jpiAnd:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecuteBool(cxt, &elem, jb);
			if (res != jperNotFound)
			{
				JsonPathExecResult res2;

				/*
				 * SQL/JSON says that we should check second arg
				 * in case of jperError
				 */

				jspGetRightArg(jsp, &elem);
				res2 = recursiveExecuteBool(cxt, &elem, jb);

				res = (res2 == jperOk) ? res : res2;
			}
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiOr:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecuteBool(cxt, &elem, jb);
			if (res != jperOk)
			{
				JsonPathExecResult res2;

				jspGetRightArg(jsp, &elem);
				res2 = recursiveExecuteBool(cxt, &elem, jb);

				res = (res2 == jperNotFound) ? res : res2;
			}
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiNot:
			jspGetArg(jsp, &elem);
			switch ((res = recursiveExecuteBool(cxt, &elem, jb)))
			{
				case jperOk:
					res = jperNotFound;
					break;
				case jperNotFound:
					res = jperOk;
					break;
				default:
					break;
			}
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiIsUnknown:
			jspGetArg(jsp, &elem);
			res = recursiveExecuteBool(cxt, &elem, jb);
			res = jperIsError(res) ? jperOk : jperNotFound;
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbValue *v;
				JsonbValue	key;
				JsonbValue	obj;

				if (jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &obj);

				key.type = jbvString;
				key.val.string.val = jspGetString(jsp, &key.val.string.len);

				v = findJsonbValueFromContainer(jb->val.binary.data, JB_FOBJECT, &key);

				if (v != NULL)
				{
					JsonValueList items = { 0 };
					JsonValueList *pitems = found;

					if (pitems && jspOutPath(jsp))
						pitems = &items;

					res = recursiveExecuteNext(cxt, jsp, NULL, v, pitems, false);

					if (jspHasNext(jsp) || !found)
						pfree(v); /* free value if it was not added to found list */

					if (!JsonValueListIsEmpty(&items) && !jperIsError(res))
						JsonValueListConcat(found, prependKey(&key, &items));
				}
				else if (!cxt->lax)
				{
					Assert(found);
					res = jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
				}
			}
			else if (!cxt->lax)
			{
				Assert(found);
				res = jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
			}
			break;
		case jpiRoot:
			jb = cxt->root;
			/* fall through */
		case jpiCurrent:
		case jpiCurrentN:
			{
				JsonbValue *v;
				JsonbValue	vbuf;
				bool		copy = true;

				if (jsp->type == jpiCurrentN)
				{
					int			i;
					JsonItemStackEntry *current = cxt->stack;

					for (i = 0; i < jsp->content.current.level; i++)
					{
						current = current->parent;

						if (!current)
							elog(ERROR,
								 "invalid jsonpath current item reference");
					}

					jb = current->item;
				}

				if (JsonbType(jb) == jbvScalar)
				{
					if (jspHasNext(jsp))
						v = &vbuf;
					else
					{
						v = palloc(sizeof(*v));
						copy = false;
					}

					JsonbExtractScalar(jb->val.binary.data, v);
				}
				else
					v = jb;

				res = recursiveExecuteNext(cxt, jsp, NULL, v, found, copy);
				break;
			}
		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				JsonValueList items = { 0 };
				JsonValueList *pitems = found;
				bool		wrap = pitems && jspOutPath(jsp);

				if (wrap)
					pitems = &items;

				hasNext = jspGetNext(jsp, &elem);

				if (jb->type == jbvArray)
				{
					JsonbValue *el = jb->val.array.elems;
					JsonbValue *last_el = el + jb->val.array.nElems;

					for (; el < last_el; el++)
					{
						res = recursiveExecuteNext(cxt, jsp, &elem, el, pitems, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
				else
				{
					JsonbValue	v;
					JsonbIterator *it;
					JsonbIteratorToken r;

					it = JsonbIteratorInit(jb->val.binary.data);

					while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
					{
						if (r == WJB_ELEM)
						{
							res = recursiveExecuteNext(cxt, jsp, &elem, &v, pitems, true);

							if (jperIsError(res))
								break;

							if (res == jperOk && !found)
								break;
						}
					}
				}

				if (wrap && !jperIsError(res))
					JsonValueListAppend(found, wrapItemsInArray(&items));
			}
			else
				res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray)
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = JsonbArraySize(jb);
				bool		binary = jb->type == jbvBinary;
				JsonValueList items = { 0 };
				JsonValueList *pitems = found;
				bool		wrap = pitems && jspOutPath(jsp);

				if (wrap)
					pitems = &items;

				cxt->innermostArraySize = size; /* for LAST evaluation */

				hasNext = jspGetNext(jsp, &elem);

				for (i = 0; i < jsp->content.array.nelems; i++)
				{
					JsonPathItem from;
					JsonPathItem to;
					int32		index;
					int32		index_from;
					int32		index_to;
					bool		range = jspGetArraySubscript(jsp, &from, &to, i);

					res = getArrayIndex(cxt, &from, jb, &index_from);

					if (jperIsError(res))
						break;

					if (range)
					{
						res = getArrayIndex(cxt, &to, jb, &index_to);

						if (jperIsError(res))
							break;
					}
					else
						index_to = index_from;

					if (!cxt->lax &&
						(index_from < 0 ||
						 index_from > index_to ||
						 index_to >= size))
					{
						res = jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);
						break;
					}

					if (index_from < 0)
						index_from = 0;

					if (index_to >= size)
						index_to = size - 1;

					res = jperNotFound;

					for (index = index_from; index <= index_to; index++)
					{
						JsonbValue *v = binary ?
							getIthJsonbValueFromContainer(jb->val.binary.data,
														  (uint32) index) :
							&jb->val.array.elems[index];

						if (v == NULL)
							continue;

						res = recursiveExecuteNext(cxt, jsp, &elem, v, pitems,
												   !binary);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;

				if (wrap && !jperIsError(res))
					JsonValueListAppend(found, wrapItemsInArray(&items));
			}
			else if (JsonbType(jb) == jbvObject)
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				JsonbValue	bin;
				JsonbValue *wrapped = NULL;
				JsonValueList items = { 0 };
				JsonValueList *pitems = found;
				bool		wrap = pitems && jspOutPath(jsp);

				if (wrap)
					pitems = &items;

				if (jb->type != jbvBinary)
					jb = JsonbWrapInBinary(jb, &bin);

				cxt->innermostArraySize = 1;

				for (i = 0; i < jsp->content.array.nelems; i++)
				{
					JsonPathItem from;
					JsonPathItem to;
					JsonbValue *key;
					JsonbValue	tmp;
					JsonValueList keys = { 0 };
					bool		range = jspGetArraySubscript(jsp, &from, &to, i);

					if (range)
					{
						int		index_from;
						int		index_to;

						if (!cxt->lax)
							return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

						if (!wrapped)
							wrapped = wrapItem(jb);

						res = getArrayIndex(cxt, &from, wrapped, &index_from);
						if (jperIsError(res))
							return res;

						res = getArrayIndex(cxt, &to, wrapped, &index_to);
						if (jperIsError(res))
							return res;

						res = jperNotFound;

						if (index_from <= 0 && index_to >= 0)
						{
							res = recursiveExecuteNext(cxt, jsp, NULL, jb,
													   pitems, true);
							if (jperIsError(res))
								return res;

						}

						if (res == jperOk && !found)
							break;

						continue;
					}

					res = recursiveExecuteNested(cxt, &from, jb, &keys);

					if (jperIsError(res))
						return res;

					if (JsonValueListLength(&keys) != 1)
						return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

					key = JsonValueListHead(&keys);

					if (JsonbType(key) == jbvScalar)
						key = JsonbExtractScalar(key->val.binary.data, &tmp);

					res = jperNotFound;

					if (key->type == jbvNumeric && cxt->lax)
					{
						int			index = DatumGetInt32(
								DirectFunctionCall1(numeric_int4,
									DirectFunctionCall2(numeric_trunc,
											NumericGetDatum(key->val.numeric),
											Int32GetDatum(0))));

						if (!index)
						{
							res = recursiveExecuteNext(cxt, jsp, NULL, jb,
													   pitems, true);
							if (jperIsError(res))
								return res;
						}
					}
					else if (key->type == jbvString)
					{
						key = findJsonbValueFromContainer(jb->val.binary.data,
														  JB_FOBJECT, key);

						if (key)
						{
							res = recursiveExecuteNext(cxt, jsp, NULL, key,
													   pitems, false);
							if (jperIsError(res))
								return res;
						}
						else if (!cxt->lax)
							return jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
					}
					else
						return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;

				if (wrap && !jperIsError(res))
					JsonValueListAppend(found, wrapItemsInArray(&items));
			}
			else
			{
				if (cxt->lax)
					res = recursiveExecuteNoUnwrap(cxt, jsp, wrapItem(jb),
												   found, false);
				else
					res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
			}
			break;

		case jpiLast:
			{
				JsonbValue	tmpjbv;
				JsonbValue *lastjbv;
				int			last;
				bool		hasNext;

				if (cxt->innermostArraySize < 0)
					elog(ERROR,
						 "evaluating jsonpath LAST outside of array subscript");

				hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
				{
					res = jperOk;
					break;
				}

				last = cxt->innermostArraySize - 1;

				lastjbv = hasNext ? &tmpjbv : palloc(sizeof(*lastjbv));

				lastjbv->type = jbvNumeric;
				lastjbv->val.numeric = DatumGetNumeric(DirectFunctionCall1(
											int4_numeric, Int32GetDatum(last)));

				res = recursiveExecuteNext(cxt, jsp, &elem, lastjbv, found, hasNext);
			}
			break;
		case jpiAnyKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbIterator	*it;
				int32			r;
				JsonbValue		v;
				JsonbValue		bin;

				if (jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &bin);

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					JsonbValue	key;
					JsonValueList items = { 0 };
					JsonValueList *pitems = found;

					if (r == WJB_KEY && jspOutPath(jsp))
					{
						key = v;
						r = JsonbIteratorNext(&it, &v, true);

						if (pitems)
							pitems = &items;
					}

					if (r == WJB_VALUE)
					{
						res = recursiveExecuteNext(cxt, jsp, &elem, &v, pitems, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;

						if (!JsonValueListIsEmpty(&items) && !jperIsError(res))
							JsonValueListConcat(found, prependKey(&key, &items));
					}
				}
			}
			else if (!cxt->lax)
			{
				Assert(found);
				res = jperMakeError(ERRCODE_JSON_OBJECT_NOT_FOUND);
			}
			break;
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			res = executeExpr(cxt, jsp, jb);
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiAdd:
		case jpiSub:
		case jpiMul:
		case jpiDiv:
		case jpiMod:
			res = executeBinaryArithmExpr(cxt, jsp, jb, found);
			break;
		case jpiPlus:
		case jpiMinus:
			res = executeUnaryArithmExpr(cxt, jsp, jb, found);
			break;
		case jpiFilter:
			jspGetArg(jsp, &elem);
			res = recursiveExecuteNested(cxt, &elem, jb, NULL);
			if (res != jperOk)
				res = jperNotFound;
			else
				res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
			break;
		case jpiAny:
			{
				JsonbValue jbvbuf;

				hasNext = jspGetNext(jsp, &elem);

				/* first try without any intermediate steps */
				if (jsp->content.anybounds.first == 0)
				{
					res = recursiveExecuteNext(cxt, jsp, &elem, jb, found, true);

					if (res == jperOk && !found)
						break;
				}

				if (jb->type == jbvArray || jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &jbvbuf);

				if (jb->type == jbvBinary)
					res = recursiveAny(cxt, hasNext ? &elem : NULL, jb, found,
									   jspOutPath(jsp), 1,
									   jsp->content.anybounds.first,
									   jsp->content.anybounds.last);
				break;
			}
		case jpiExists:
			jspGetArg(jsp, &elem);

			if (cxt->lax)
				res = recursiveExecute(cxt, &elem, jb, NULL);
			else
			{
				JsonValueList vals = { 0 };

				/*
				 * In strict mode we must get a complete list of values
				 * to check that there are no errors at all.
				 */
				res = recursiveExecute(cxt, &elem, jb, &vals);

				if (!jperIsError(res))
					res = JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
			}

			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
		case jpiArgument:
			{
				JsonbValue	vbuf;
				JsonbValue *v;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
				{
					res = jperOk; /* skip evaluation */
					break;
				}

				v = hasNext ? &vbuf : palloc(sizeof(*v));

				computeJsonPathItem(cxt, jsp, v);

				res = recursiveExecuteNext(cxt, jsp, &elem, v, found, hasNext);
			}
			break;
		case jpiType:
			{
				JsonbValue *jbv = palloc(sizeof(*jbv));

				jbv->type = jbvString;
				jbv->val.string.val = pstrdup(JsonbTypeName(jb));
				jbv->val.string.len = strlen(jbv->val.string.val);

				res = recursiveExecuteNext(cxt, jsp, NULL, jbv, found, false);
			}
			break;
		case jpiSize:
			{
				int			size = JsonbArraySize(jb);

				if (size < 0)
				{
					if (!cxt->lax)
					{
						res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
						break;
					}

					size = 1;
				}

				jb = palloc(sizeof(*jb));

				jb->type = jbvNumeric;
				jb->val.numeric =
					DatumGetNumeric(DirectFunctionCall1(int4_numeric,
														Int32GetDatum(size)));

				res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, false);
			}
			break;
		case jpiAbs:
		case jpiFloor:
		case jpiCeiling:
			{
				JsonbValue jbvbuf;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbvbuf);

				if (jb->type == jbvNumeric)
				{
					Datum		datum = NumericGetDatum(jb->val.numeric);

					switch (jsp->type)
					{
						case jpiAbs:
							datum = DirectFunctionCall1(numeric_abs, datum);
							break;
						case jpiFloor:
							datum = DirectFunctionCall1(numeric_floor, datum);
							break;
						case jpiCeiling:
							datum = DirectFunctionCall1(numeric_ceil, datum);
							break;
						default:
							break;
					}

					jb = palloc(sizeof(*jb));

					jb->type = jbvNumeric;
					jb->val.numeric = DatumGetNumeric(datum);

					res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, false);
				}
				else
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
			}
			break;
		case jpiDouble:
			{
				JsonbValue jbv;
				MemoryContext mcxt = CurrentMemoryContext;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbv);

				PG_TRY();
				{
					if (jb->type == jbvNumeric)
					{
						/* only check success of numeric to double cast */
						DirectFunctionCall1(numeric_float8,
											NumericGetDatum(jb->val.numeric));
						res = jperOk;
					}
					else if (jb->type == jbvString)
					{
						/* cast string as double */
						char	   *str = pnstrdup(jb->val.string.val,
												   jb->val.string.len);
						Datum		val = DirectFunctionCall1(
											float8in, CStringGetDatum(str));
						pfree(str);

						jb = &jbv;
						jb->type = jbvNumeric;
						jb->val.numeric = DatumGetNumeric(DirectFunctionCall1(
														float8_numeric, val));
						res = jperOk;

					}
					else
						res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
				}
				PG_CATCH();
				{
					if (ERRCODE_TO_CATEGORY(geterrcode()) !=
														ERRCODE_DATA_EXCEPTION)
						PG_RE_THROW();

					FlushErrorState();
					MemoryContextSwitchTo(mcxt);
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
				}
				PG_END_TRY();

				if (res == jperOk)
					res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
			}
			break;
		case jpiDatetime:
			{
				JsonbValue	jbvbuf;
				Datum		value;
				Oid			typid;
				int32		typmod = -1;
				bool		hasNext;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbvbuf);

				if (jb->type == jbvNumeric && !jsp->content.arg)
				{
					/* Standard extension: unix epoch to timestamptz */
					MemoryContext mcxt = CurrentMemoryContext;

					PG_TRY();
					{
						Datum		unix_epoch =
								DirectFunctionCall1(numeric_float8,
											NumericGetDatum(jb->val.numeric));

						value = DirectFunctionCall1(float8_timestamptz,
													unix_epoch);
						typid = TIMESTAMPTZOID;
						res = jperOk;
					}
					PG_CATCH();
					{
						if (ERRCODE_TO_CATEGORY(geterrcode()) !=
														ERRCODE_DATA_EXCEPTION)
							PG_RE_THROW();

						FlushErrorState();
						MemoryContextSwitchTo(mcxt);

						res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
					}
					PG_END_TRY();
				}
				else if (jb->type == jbvString)
				{

					text	   *datetime_txt =
							cstring_to_text_with_len(jb->val.string.val,
													 jb->val.string.len);

					res = jperOk;

					if (jsp->content.arg)
					{
						text	   *template_txt;
						char	   *template_str;
						int			template_len;
						MemoryContext mcxt = CurrentMemoryContext;

						jspGetArg(jsp, &elem);

						if (elem.type != jpiString)
							elog(ERROR, "invalid jsonpath item type for .datetime() argument");

						template_str = jspGetString(&elem, &template_len);
						template_txt = cstring_to_text_with_len(template_str,
																template_len);

						PG_TRY();
						{
							value = to_datetime(datetime_txt,
												template_str, template_len,
												false,
												&typid, &typmod);
						}
						PG_CATCH();
						{
							if (ERRCODE_TO_CATEGORY(geterrcode()) !=
														ERRCODE_DATA_EXCEPTION)
								PG_RE_THROW();

							FlushErrorState();
							MemoryContextSwitchTo(mcxt);

							res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
						}
						PG_END_TRY();

						pfree(template_txt);
					}
					else
					{
						if (!tryToParseDatetime("yyyy-mm-dd HH24:MI:SS TZH:TZM",
									datetime_txt, &value, &typid, &typmod) &&
							!tryToParseDatetime("yyyy-mm-dd HH24:MI:SS TZH",
									datetime_txt, &value, &typid, &typmod) &&
							!tryToParseDatetime("yyyy-mm-dd HH24:MI:SS",
									datetime_txt, &value, &typid, &typmod) &&
							!tryToParseDatetime("yyyy-mm-dd",
									datetime_txt, &value, &typid, &typmod) &&
							!tryToParseDatetime("HH24:MI:SS TZH:TZM",
									datetime_txt, &value, &typid, &typmod) &&
							!tryToParseDatetime("HH24:MI:SS TZH",
									datetime_txt, &value, &typid, &typmod) &&
							!tryToParseDatetime("HH24:MI:SS",
									datetime_txt, &value, &typid, &typmod))
							res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
					}

					pfree(datetime_txt);
				}
				else
				{
					res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
					break;
				}

				if (jperIsError(res))
					break;

				hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
					break;

				jb = hasNext ? &jbvbuf : palloc(sizeof(*jb));

				jb->type = jbvDatetime;
				jb->val.datetime.value = value;
				jb->val.datetime.typid = typid;
				jb->val.datetime.typmod = typmod;

				res = recursiveExecuteNext(cxt, jsp, &elem, jb, found, hasNext);
			}
			break;
		case jpiKeyValue:
			if (JsonbType(jb) != jbvObject)
				res = jperMakeError(ERRCODE_JSON_OBJECT_NOT_FOUND);
			else
			{
				int32		r;
				JsonbValue	bin;
				JsonbValue	key;
				JsonbValue	val;
				JsonbValue	obj;
				JsonbValue	keystr;
				JsonbValue	valstr;
				JsonbIterator *it;
				JsonbParseState *ps = NULL;

				hasNext = jspGetNext(jsp, &elem);

				if (jb->type == jbvBinary
					? !JsonContainerSize(jb->val.binary.data)
					: !jb->val.object.nPairs)
				{
					res = jperNotFound;
					break;
				}

				/* make template object */
				obj.type = jbvBinary;

				keystr.type = jbvString;
				keystr.val.string.val = "key";
				keystr.val.string.len = 3;

				valstr.type = jbvString;
				valstr.val.string.val = "value";
				valstr.val.string.len = 5;

				if (jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &bin);

				it = JsonbIteratorInit(jb->val.binary.data);

				while ((r = JsonbIteratorNext(&it, &key, true)) != WJB_DONE)
				{
					if (r == WJB_KEY)
					{
						Jsonb	   *jsonb;
						JsonbValue  *keyval;

						res = jperOk;

						if (!hasNext && !found)
							break;

						r = JsonbIteratorNext(&it, &val, true);
						Assert(r == WJB_VALUE);

						pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

						pushJsonbValue(&ps, WJB_KEY, &keystr);
						pushJsonbValue(&ps, WJB_VALUE, &key);


						pushJsonbValue(&ps, WJB_KEY, &valstr);
						pushJsonbValue(&ps, WJB_VALUE, &val);

						keyval = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

						jsonb = JsonbValueToJsonb(keyval);

						JsonbInitBinary(&obj, jsonb);

						res = recursiveExecuteNext(cxt, jsp, &elem, &obj, found, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
			}
			break;
		case jpiStartsWith:
			res = executeStartsWithPredicate(cxt, jsp, jb);
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiLikeRegex:
			res = executeLikeRegexPredicate(cxt, jsp, jb);
			res = appendBoolResult(cxt, jsp, found, res, needBool);
			break;
		case jpiSequence:
		{
			JsonPathItem next;
			bool		hasNext = jspGetNext(jsp, &next);
			JsonValueList list;
			JsonValueList *plist = hasNext ? &list : found;
			JsonValueListIterator it;
			int			i;

			for (i = 0; i < jsp->content.sequence.nelems; i++)
			{
				JsonbValue *v;

				if (hasNext)
					memset(&list, 0, sizeof(list));

				jspGetSequenceElement(jsp, i, &elem);
				res = recursiveExecute(cxt, &elem, jb, plist);

				if (jperIsError(res))
					break;

				if (!hasNext)
				{
					if (!found && res == jperOk)
						break;
					continue;
				}

				memset(&it, 0, sizeof(it));

				while ((v = JsonValueListNext(&list, &it)))
				{
					res = recursiveExecute(cxt, &next, v, found);

					if (jperIsError(res) || (!found && res == jperOk))
					{
						i = jsp->content.sequence.nelems;
						break;
					}
				}
			}

			break;
		}
		case jpiArray:
			{
				JsonValueList list = { 0 };

				if (jsp->content.arg)
				{
					jspGetArg(jsp, &elem);
					res = recursiveExecute(cxt, &elem, jb, &list);

					if (jperIsError(res))
						break;
				}

				res = recursiveExecuteNext(cxt, jsp, NULL,
										   wrapItemsInArray(&list),
										   found, false);
			}
			break;
		case jpiObject:
			{
				JsonbParseState *ps = NULL;
				JsonbValue *obj;
				int			i;

				pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

				for (i = 0; i < jsp->content.object.nfields; i++)
				{
					JsonbValue *jbv;
					JsonbValue	jbvtmp;
					JsonPathItem key;
					JsonPathItem val;
					JsonValueList key_list = { 0 };
					JsonValueList val_list = { 0 };

					jspGetObjectField(jsp, i, &key, &val);

					recursiveExecute(cxt, &key, jb, &key_list);

					if (JsonValueListLength(&key_list) != 1)
					{
						res = jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);
						break;
					}

					jbv = JsonValueListHead(&key_list);

					if (JsonbType(jbv) == jbvScalar)
						jbv = JsonbExtractScalar(jbv->val.binary.data, &jbvtmp);

					if (jbv->type != jbvString)
					{
						res = jperMakeError(ERRCODE_JSON_SCALAR_REQUIRED); /* XXX */
						break;
					}

					pushJsonbValue(&ps, WJB_KEY, jbv);

					recursiveExecute(cxt, &val, jb, &val_list);

					if (JsonValueListLength(&val_list) != 1)
					{
						res = jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);
						break;
					}

					jbv = JsonValueListHead(&val_list);

					if (jbv->type == jbvObject || jbv->type == jbvArray)
						jbv = JsonbWrapInBinary(jbv, &jbvtmp);

					pushJsonbValue(&ps, WJB_VALUE, jbv);
				}

				if (jperIsError(res))
					break;

				obj = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

				res = recursiveExecuteNext(cxt, jsp, NULL, obj, found, false);
			}
			break;
		case jpiLambda:
			elog(ERROR, "unable to directly execute jsonpath lambda expression");
			break;
		case jpiMethod:
		case jpiFunction:
			res = executeFunction(cxt, jsp, jb, found, needBool);
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
static JsonPathExecResult
recursiveExecuteUnwrapArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
							JsonbValue *jb, JsonValueList *found)
{
	JsonPathExecResult res = jperNotFound;

	if (jb->type == jbvArray)
	{
		JsonbValue *elem = jb->val.array.elems;
		JsonbValue *last = elem + jb->val.array.nElems;

		for (; elem < last; elem++)
		{
			res = recursiveExecuteNoUnwrap(cxt, jsp, elem, found, false);

			if (jperIsError(res))
				break;
			if (res == jperOk && !found)
				break;
		}
	}
	else
	{
		JsonbValue	v;
		JsonbIterator *it;
		JsonbIteratorToken tok;

		it = JsonbIteratorInit(jb->val.binary.data);

		while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
		{
			if (tok == WJB_ELEM)
			{
				res = recursiveExecuteNoUnwrap(cxt, jsp, &v, found, false);
				if (jperIsError(res))
					break;
				if (res == jperOk && !found)
					break;
			}
		}
	}

	return res;
}

/*
 * Execute jsonpath with unwrapping of current item if it is an array.
 */
static inline JsonPathExecResult
recursiveExecuteUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, JsonValueList *found)
{
	if (cxt->lax && JsonbType(jb) == jbvArray)
		return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

	return recursiveExecuteNoUnwrap(cxt, jsp, jb, found, false);
}

/*
 * Wrap a non-array SQL/JSON item into an array for applying array subscription
 * path steps in lax mode.
 */
static inline JsonbValue *
wrapItem(JsonbValue *jbv)
{
	JsonbParseState *ps = NULL;
	JsonbValue	jbvbuf;

	switch (JsonbType(jbv))
	{
		case jbvArray:
			/* Simply return an array item. */
			return jbv;

		case jbvScalar:
			/* Extract scalar value from singleton pseudo-array. */
			jbv = JsonbExtractScalar(jbv->val.binary.data, &jbvbuf);
			break;

		case jbvObject:
			/*
			 * Need to wrap object into a binary JsonbValue for its unpacking
			 * in pushJsonbValue().
			 */
			if (jbv->type != jbvBinary)
				jbv = JsonbWrapInBinary(jbv, &jbvbuf);
			break;

		default:
			/* Ordinary scalars can be pushed directly. */
			break;
	}

	pushJsonbValue(&ps, WJB_BEGIN_ARRAY, NULL);
	pushJsonbValue(&ps, WJB_ELEM, jbv);
	jbv = pushJsonbValue(&ps, WJB_END_ARRAY, NULL);

	return JsonbWrapInBinary(jbv, NULL);
}

JsonbValue *
JsonbWrapItemInArray(JsonbValue *item)
{
	return wrapItem(item);
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
static inline JsonPathExecResult
recursiveExecute(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
				 JsonValueList *found)
{
	if (cxt->lax)
	{
		switch (jsp->type)
		{
			case jpiKey:
			case jpiAnyKey:
		/*	case jpiAny: */
			case jpiFilter:
			/* all methods excluding type() and size() */
			case jpiAbs:
			case jpiFloor:
			case jpiCeiling:
			case jpiDouble:
			case jpiDatetime:
			case jpiKeyValue:
				return recursiveExecuteUnwrap(cxt, jsp, jb, found);

			case jpiAnyArray:
				jb = wrapItem(jb);
				break;

			default:
				break;
		}
	}

	return recursiveExecuteNoUnwrap(cxt, jsp, jb, found, false);
}

JsonPathExecResult
jspRecursiveExecute(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
					JsonValueList *found)
{
	return recursiveExecute(cxt, jsp, jb, found);
}

/*
 * Execute boolean-valued jsonpath expression.  Boolean items are not appended
 * to the result list, only return code determines result:
 *  - jperOk => true
 *  - jperNotFound => false
 *  - jperError => NULL (errors are converted to NULL values)
 */
static inline JsonPathExecResult
recursiveExecuteBool(JsonPathExecContext *cxt, JsonPathItem *jsp,
					 JsonbValue *jb)
{
	if (jspHasNext(jsp))
		elog(ERROR, "boolean jsonpath item can not have next item");

	switch (jsp->type)
	{
		case jpiAnd:
		case jpiOr:
		case jpiNot:
		case jpiIsUnknown:
		case jpiEqual:
		case jpiNotEqual:
		case jpiGreater:
		case jpiGreaterOrEqual:
		case jpiLess:
		case jpiLessOrEqual:
		case jpiExists:
		case jpiStartsWith:
		case jpiLikeRegex:
			break;

		default:
			elog(ERROR, "invalid boolean jsonpath item type: %d", jsp->type);
			break;
	}

	return recursiveExecuteNoUnwrap(cxt, jsp, jb, NULL, true);
}

/*
 * Public interface to jsonpath executor
 */
JsonPathExecResult
executeJsonPath(JsonPath *path, List *vars, Jsonb *json,
				JsonValueList *foundJson, void **pCache, MemoryContext cacheCxt)
{
	JsonPathExecContext cxt;
	JsonPathItem	jsp;
	JsonbValue		jbv;
	JsonItemStackEntry root;

	jspInit(&jsp, path);

	cxt.vars = vars;
	cxt.args = NULL;
	cxt.lax = (path->header & JSONPATH_LAX) != 0;
	cxt.root = JsonbInitBinary(&jbv, json);
	cxt.stack = NULL;
	cxt.innermostArraySize = -1;

	if (path->ext_items_count)
	{
		if (pCache)
		{
			struct
			{
				JsonPath   *path;
				void	  **cache;
			} *cache = *pCache;

			if (!cache)
				cache = *pCache = MemoryContextAllocZero(cacheCxt, sizeof(*cache));

			if (cache->path &&
				(VARSIZE(path) != VARSIZE(cache->path) ||
				 memcmp(path, cache->path, VARSIZE(path))))
			{
				/* invalidate cache TODO optimize */
				cache->path = NULL;

				if (cache->cache)
				{
					pfree(cache->cache);
					cache->cache = NULL;
				}
			}

			if (cache->path)
			{
				Assert(cache->cache);
			}
			else
			{
				Assert(!cache->cache);

				cache->path = MemoryContextAlloc(cacheCxt, VARSIZE(path));
				memcpy(cache->path, path, VARSIZE(path));

				cache->cache = MemoryContextAllocZero(cacheCxt,
													  sizeof(cxt.cache[0]) *
													  path->ext_items_count);
			}

			cxt.cache = cache->cache;
			cxt.cache_mcxt = cacheCxt;

			path = cache->path;		/* use cached jsonpath value */
		}
		else
		{
			cxt.cache = palloc0(sizeof(cxt.cache[0]) * path->ext_items_count);
			cxt.cache_mcxt = CurrentMemoryContext;
		}
	}
	else
	{
		cxt.cache = NULL;
		cxt.cache_mcxt = NULL;
	}

	pushJsonItem(&cxt.stack, &root, cxt.root);

	jspInit(&jsp, path);

	if (!cxt.lax && !foundJson)
	{
		/*
		 * In strict mode we must get a complete list of values to check
		 * that there are no errors at all.
		 */
		JsonValueList vals = { 0 };
		JsonPathExecResult res = recursiveExecute(&cxt, &jsp, &jbv, &vals);

		if (jperIsError(res))
			return res;

		return JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
	}

	return recursiveExecute(&cxt, &jsp, &jbv, foundJson);
}

static Datum
returnDATUM(void *arg, bool *isNull)
{
	*isNull = false;
	return	PointerGetDatum(arg);
}

static Datum
returnNULL(void *arg, bool *isNull)
{
	*isNull = true;
	return Int32GetDatum(0);
}

/*
 * Convert jsonb object into list of vars for executor
 */
static List*
makePassingVars(Jsonb *jb)
{
	JsonbValue		v;
	JsonbIterator	*it;
	int32			r;
	List			*vars = NIL;

	it = JsonbIteratorInit(&jb->root);

	r =  JsonbIteratorNext(&it, &v, true);

	if (r != WJB_BEGIN_OBJECT)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("passing variable json is not a object")));

	while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			JsonPathVariable	*jpv = palloc0(sizeof(*jpv));

			jpv->varName = cstring_to_text_with_len(v.val.string.val,
													v.val.string.len);

			JsonbIteratorNext(&it, &v, true);

			jpv->cb = returnDATUM;

			switch(v.type)
			{
				case jbvBool:
					jpv->typid = BOOLOID;
					jpv->cb_arg = DatumGetPointer(BoolGetDatum(v.val.boolean));
					break;
				case jbvNull:
					jpv->cb = returnNULL;
					break;
				case jbvString:
					jpv->typid = TEXTOID;
					jpv->cb_arg = cstring_to_text_with_len(v.val.string.val,
														   v.val.string.len);
					break;
				case jbvNumeric:
					jpv->typid = NUMERICOID;
					jpv->cb_arg = v.val.numeric;
					break;
				case jbvBinary:
					jpv->typid = JSONXOID;
					jpv->cb_arg = DatumGetPointer(JsonbPGetDatum(JsonbValueToJsonb(&v)));
					break;
				default:
					elog(ERROR, "unsupported type in passing variable json");
			}

			vars = lappend(vars, jpv);
		}
	}

	return vars;
}

static void
throwJsonPathError(JsonPathExecResult res)
{
	if (!jperIsError(res))
		return;

	switch (jperGetError(res))
	{
		case ERRCODE_JSON_ARRAY_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON array not found")));
			break;
		case ERRCODE_JSON_OBJECT_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON object not found")));
			break;
		case ERRCODE_JSON_MEMBER_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON member not found")));
			break;
		case ERRCODE_JSON_NUMBER_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON number not found")));
			break;
		case ERRCODE_JSON_SCALAR_REQUIRED:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON scalar required")));
			break;
		case ERRCODE_SINGLETON_JSON_ITEM_REQUIRED:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Singleton SQL/JSON item required")));
			break;
		case ERRCODE_NON_NUMERIC_JSON_ITEM:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Non-numeric SQL/JSON item")));
			break;
		case ERRCODE_INVALID_JSON_SUBSCRIPT:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Invalid SQL/JSON subscript")));
			break;
		case ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Invalid argument for SQL/JSON datetime function")));
			break;
		default:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Unknown SQL/JSON error")));
			break;
	}
}

static Datum
jsonb_jsonpath_exists(PG_FUNCTION_ARGS)
{
	Jsonb				*jb = PG_GETARG_JSONB_P(0);
	JsonPath			*jp = PG_GETARG_JSONPATH_P(1);
	JsonPathExecResult	res;
	List				*vars = NIL;

	if (PG_NARGS() == 3)
		vars = makePassingVars(PG_GETARG_JSONB_P(2));

	res = executeJsonPath(jp, vars, jb, NULL,
						  &fcinfo->flinfo->fn_extra, fcinfo->flinfo->fn_mcxt);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	throwJsonPathError(res);

	PG_RETURN_BOOL(res == jperOk);
}

Datum
jsonb_jsonpath_exists2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_exists(fcinfo);
}

Datum
jsonb_jsonpath_exists3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_exists(fcinfo);
}

static inline Datum
jsonb_jsonpath_predicate(FunctionCallInfo fcinfo, List *vars)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonbValue *jbv;
	JsonValueList found = { 0 };
	JsonPathExecResult res;

	res = executeJsonPath(jp, vars, jb, &found,
						  &fcinfo->flinfo->fn_extra,
						  fcinfo->flinfo->fn_mcxt);

	throwJsonPathError(res);

	if (JsonValueListLength(&found) != 1)
		throwJsonPathError(jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED));

	jbv = JsonValueListHead(&found);

	if (JsonbType(jbv) == jbvScalar)
		JsonbExtractScalar(jbv->val.binary.data, jbv);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (jbv->type == jbvNull)
		PG_RETURN_NULL();

	if (jbv->type != jbvBool)
		PG_RETURN_NULL(); /* XXX */

	PG_RETURN_BOOL(jbv->val.boolean);
}

Datum
jsonb_jsonpath_predicate2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_predicate(fcinfo, NIL);
}

Datum
jsonb_jsonpath_predicate3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_predicate(fcinfo,
									makePassingVars(PG_GETARG_JSONB_P(2)));
}

typedef struct JsonpathQueryContext
{
	void			*cache;		/* jsonpath executor cache */
	FuncCallContext	*srfcxt;	/* SRF context */
} JsonpathQueryContext;

static Datum
jsonb_jsonpath_query(FunctionCallInfo fcinfo)
{
	FuncCallContext	*funcctx;
	List			*found;
	JsonbValue		*v;
	ListCell		*c;
	JsonpathQueryContext *cxt = fcinfo->flinfo->fn_extra;

	if (!cxt)
		cxt = fcinfo->flinfo->fn_extra =
			MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*cxt));

	if (SRF_IS_FIRSTCALL_EXT(&cxt->srfcxt))
	{
		JsonPath			*jp = PG_GETARG_JSONPATH_P(1);
		Jsonb				*jb;
		JsonPathExecResult	res;
		MemoryContext		oldcontext;
		List				*vars = NIL;
		JsonValueList		found = { 0 };

		funcctx = SRF_FIRSTCALL_INIT_EXT(&cxt->srfcxt);
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		jb = PG_GETARG_JSONB_P_COPY(0);
		if (PG_NARGS() == 3)
			vars = makePassingVars(PG_GETARG_JSONB_P(2));

		res = executeJsonPath(jp, vars, jb, &found, &cxt->cache,
							  fcinfo->flinfo->fn_mcxt);

		if (jperIsError(res))
			throwJsonPathError(res);

		PG_FREE_IF_COPY(jp, 1);

		funcctx->user_fctx = JsonValueListGetList(&found);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP_EXT(&cxt->srfcxt);
	found = funcctx->user_fctx;

	c = list_head(found);

	if (c == NULL)
		SRF_RETURN_DONE_EXT(funcctx, &cxt->srfcxt);

	v = lfirst(c);
	funcctx->user_fctx = list_delete_first(found);

	SRF_RETURN_NEXT(funcctx, JsonbPGetDatum(JsonbValueToJsonb(v)));
}

Datum
jsonb_jsonpath_query2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_query(fcinfo);
}

Datum
jsonb_jsonpath_query3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_query(fcinfo);
}

/* Construct a JSON array from the item list */
static inline JsonbValue *
wrapItemsInArray(const JsonValueList *items)
{
	JsonbParseState *ps = NULL;
	JsonValueListIterator it = { 0 };
	JsonbValue *jbv;

	pushJsonbValue(&ps, WJB_BEGIN_ARRAY, NULL);

	while ((jbv = JsonValueListNext(items, &it)))
	{
		JsonbValue	bin;

		if (jbv->type == jbvBinary &&
			JsonContainerIsScalar(jbv->val.binary.data))
			JsonbExtractScalar(jbv->val.binary.data, jbv);

		if (jbv->type == jbvObject || jbv->type == jbvArray)
			jbv = JsonbWrapInBinary(jbv, &bin);

		pushJsonbValue(&ps, WJB_ELEM, jbv);
	}

	return pushJsonbValue(&ps, WJB_END_ARRAY, NULL);
}

JsonbValue *
JsonbWrapItemsInArray(const JsonValueList *items)
{
	return wrapItemsInArray(items);
}

/********************Interface to pgsql's executor***************************/
bool
JsonbPathExists(Datum jb, JsonPath *jp, List *vars)
{
	JsonPathExecResult res = executeJsonPath(jp, vars, DatumGetJsonbP(jb),
											 NULL, NULL, NULL);

	throwJsonPathError(res);

	return res == jperOk;
}

Datum
JsonbPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper,
			   bool *empty, List *vars)
{
	JsonbValue *first;
	bool		wrap;
	JsonValueList found = { 0 };
	JsonPathExecResult jper = executeJsonPath(jp, vars, DatumGetJsonbP(jb),
											  &found, NULL, NULL);
	int			count;

	throwJsonPathError(jper);

	count = JsonValueListLength(&found);

	first = count ? JsonValueListHead(&found) : NULL;

	if (!first)
		wrap = false;
	else if (wrapper == JSW_NONE)
		wrap = false;
	else if (wrapper == JSW_UNCONDITIONAL)
		wrap = true;
	else if (wrapper == JSW_CONDITIONAL)
		wrap = count > 1 ||
			IsAJsonbScalar(first) ||
			(first->type == jbvBinary &&
			 JsonContainerIsScalar(first->val.binary.data));
	else
	{
		elog(ERROR, "unrecognized json wrapper %d", wrapper);
		wrap = false;
	}

	if (wrap)
		return JsonbPGetDatum(JsonbValueToJsonb(wrapItemsInArray(&found)));

	if (count > 1)
		ereport(ERROR,
				(errcode(ERRCODE_MORE_THAN_ONE_JSON_ITEM),
				 errmsg("more than one SQL/JSON item")));

	if (first)
		return JsonbPGetDatum(JsonbValueToJsonb(first));

	*empty = true;
	return PointerGetDatum(NULL);
}

JsonbValue *
JsonbPathValue(Datum jb, JsonPath *jp, bool *empty, List *vars)
{
	JsonbValue *res;
	JsonValueList found = { 0 };
	JsonPathExecResult jper = executeJsonPath(jp, vars, DatumGetJsonbP(jb),
											  &found, NULL, NULL);
	int			count;

	throwJsonPathError(jper);

	count = JsonValueListLength(&found);

	*empty = !count;

	if (*empty)
		return NULL;

	if (count > 1)
		ereport(ERROR,
				(errcode(ERRCODE_MORE_THAN_ONE_JSON_ITEM),
				 errmsg("more than one SQL/JSON item")));

	res = JsonValueListHead(&found);

	if (res->type == jbvBinary &&
		JsonContainerIsScalar(res->val.binary.data))
		JsonbExtractScalar(res->val.binary.data, res);

	if (!IsAJsonbScalar(res))
		ereport(ERROR,
				(errcode(ERRCODE_JSON_SCALAR_REQUIRED),
				 errmsg("SQL/JSON scalar required")));

	if (res->type == jbvNull)
		return NULL;

	return res;
}

/*
 * Returns private data from executor state. Ensure validity by check with
 * MAGIC number.
 */
static inline JsonTableContext *
GetJsonTableContext(TableFuncScanState *state, const char *fname)
{
	JsonTableContext *result;

	if (!IsA(state, TableFuncScanState))
		elog(ERROR, "%s called with invalid TableFuncScanState", fname);
	result = (JsonTableContext *) state->opaque;
	if (result->magic != JSON_TABLE_CONTEXT_MAGIC)
		elog(ERROR, "%s called with invalid TableFuncScanState", fname);

	return result;
}

/* Recursively initialize JSON_TABLE scan state */
static void
JsonTableInitScanState(JsonTableContext *cxt, JsonTableScanState *scan,
					   JsonTableParentNode *node, JsonTableScanState *parent,
					   List *args, MemoryContext mcxt)
{
	int			i;

	scan->parent = parent;
	scan->outerJoin = node->outerJoin;
	scan->errorOnError = node->errorOnError;
	scan->path = DatumGetJsonPathP(node->path->constvalue);
	scan->args = args;
	scan->mcxt = AllocSetContextCreate(mcxt, "JsonTableContext",
									   ALLOCSET_DEFAULT_SIZES);
	scan->nested = node->child ?
		JsonTableInitPlanState(cxt, node->child, scan) : NULL;

	for (i = node->colMin; i <= node->colMax; i++)
		cxt->colexprs[i].scan = scan;
}

/* Recursively initialize JSON_TABLE scan state */
static JsonTableJoinState *
JsonTableInitPlanState(JsonTableContext *cxt, Node *plan,
					   JsonTableScanState *parent)
{
	JsonTableJoinState *state = palloc0(sizeof(*state));

	if (IsA(plan, JsonTableSiblingNode))
	{
		JsonTableSiblingNode *join = castNode(JsonTableSiblingNode, plan);

		state->is_join = true;
		state->u.join.cross = join->cross;
		state->u.join.left = JsonTableInitPlanState(cxt, join->larg, parent);
		state->u.join.right = JsonTableInitPlanState(cxt, join->rarg, parent);
	}
	else
	{
		JsonTableParentNode *node = castNode(JsonTableParentNode, plan);

		state->is_join = false;

		JsonTableInitScanState(cxt, &state->u.scan, node, parent,
							   parent->args, parent->mcxt);
	}

	return state;
}

/*
 * JsonTableInitOpaque
 *		Fill in TableFuncScanState->opaque for JsonTable processor
 */
static void
JsonTableInitOpaque(TableFuncScanState *state, int natts)
{
	JsonTableContext *cxt;
	PlanState  *ps = &state->ss.ps;
	TableFuncScan  *tfs = castNode(TableFuncScan, ps->plan);
	TableFunc  *tf = tfs->tablefunc;
	JsonExpr   *ci = castNode(JsonExpr, tf->docexpr);
	JsonTableParentNode *root = castNode(JsonTableParentNode, tf->plan);
	List	   *args = NIL;
	ListCell   *lc;
	int			i;

	cxt = palloc0(sizeof(JsonTableContext));
	cxt->magic = JSON_TABLE_CONTEXT_MAGIC;

	if (list_length(ci->passing.values) > 0)
	{
		ListCell   *exprlc;
		ListCell   *namelc;

		forboth(exprlc, ci->passing.values,
				namelc, ci->passing.names)
		{
			Expr	   *expr = (Expr *) lfirst(exprlc);
			Value	   *name = (Value *) lfirst(namelc);
			JsonPathVariableEvalContext *var = palloc(sizeof(*var));

			var->var.varName = cstring_to_text(name->val.str);
			var->var.typid = exprType((Node *) expr);
			var->var.typmod = exprTypmod((Node *) expr);
			var->var.cb = EvalJsonPathVar;
			var->var.cb_arg = var;
			var->estate = ExecInitExpr(expr, ps);
			var->econtext = ps->ps_ExprContext;
			var->mcxt = CurrentMemoryContext;
			var->evaluated = false;
			var->value = (Datum) 0;
			var->isnull = true;

			args = lappend(args, var);
		}
	}

	cxt->colexprs = palloc(sizeof(*cxt->colexprs) *
						   list_length(tf->colvalexprs));

	i = 0;

	foreach(lc, tf->colvalexprs)
	{
		Expr *expr = lfirst(lc);

		cxt->colexprs[i++].expr = ExecInitExpr(expr, ps);
	}

	JsonTableInitScanState(cxt, &cxt->root, root, NULL, args,
						   CurrentMemoryContext);

	state->opaque = cxt;
}

/* Reset scan iterator to the beginning of the item list */
static void
JsonTableRescan(JsonTableScanState *scan)
{
	memset(&scan->iter, 0, sizeof(scan->iter));
	scan->current = PointerGetDatum(NULL);
	scan->advanceNested = false;
	scan->ordinal = 0;
}

/* Reset context item of a scan, execute JSON path and reset a scan */
static void
JsonTableResetContextItem(JsonTableScanState *scan, Datum item)
{
	MemoryContext oldcxt;
	JsonPathExecResult res;

	JsonValueListClear(&scan->found);

	MemoryContextResetOnly(scan->mcxt);

	oldcxt = MemoryContextSwitchTo(scan->mcxt);

	res = executeJsonPath(scan->path, scan->args, DatumGetJsonbP(item),
						  &scan->found, NULL /* FIXME */, NULL);

	MemoryContextSwitchTo(oldcxt);

	if (jperIsError(res))
	{
		if (scan->errorOnError)
			throwJsonPathError(res);	/* does not return */
		else
			JsonValueListClear(&scan->found);	/* EMPTY ON ERROR case */
	}

	JsonTableRescan(scan);
}

/*
 * JsonTableSetDocument
 *		Install the input document
 */
static void
JsonTableSetDocument(TableFuncScanState *state, Datum value)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableSetDocument");

	JsonTableResetContextItem(&cxt->root, value);
}

/* Recursively reset scan and its child nodes */
static void
JsonTableRescanRecursive(JsonTableJoinState *state)
{
	if (state->is_join)
	{
		JsonTableRescanRecursive(state->u.join.left);
		JsonTableRescanRecursive(state->u.join.right);
		state->u.join.advanceRight = false;
	}
	else
	{
		JsonTableRescan(&state->u.scan);
		if (state->u.scan.nested)
			JsonTableRescanRecursive(state->u.scan.nested);
	}
}

/*
 * Fetch next row from a cross/union joined scan.
 *
 * Returned false at the end of a scan, true otherwise.
 */
static bool
JsonTableNextJoinRow(JsonTableJoinState *state)
{
	if (!state->is_join)
		return JsonTableNextRow(&state->u.scan);

	if (state->u.join.advanceRight)
	{
		/* fetch next inner row */
		if (JsonTableNextJoinRow(state->u.join.right))
			return true;

		/* inner rows are exhausted */
		if (state->u.join.cross)
			state->u.join.advanceRight = false;	/* next outer row */
		else
			return false;	/* end of scan */
	}

	while (!state->u.join.advanceRight)
	{
		/* fetch next outer row */
		bool		left = JsonTableNextJoinRow(state->u.join.left);

		if (state->u.join.cross)
		{
			if (!left)
				return false;	/* end of scan */

			JsonTableRescanRecursive(state->u.join.right);

			if (!JsonTableNextJoinRow(state->u.join.right))
				continue;	/* next outer row */

			state->u.join.advanceRight = true;	/* next inner row */
		}
		else if (!left)
		{
			if (!JsonTableNextJoinRow(state->u.join.right))
				return false;	/* end of scan */

			state->u.join.advanceRight = true;	/* next inner row */
		}

		break;
	}

	return true;
}

/* Recursively set 'reset' flag of scan and its child nodes */
static void
JsonTableJoinReset(JsonTableJoinState *state)
{
	if (state->is_join)
	{
		JsonTableJoinReset(state->u.join.left);
		JsonTableJoinReset(state->u.join.right);
		state->u.join.advanceRight = false;
	}
	else
	{
		state->u.scan.reset = true;
		state->u.scan.advanceNested = false;

		if (state->u.scan.nested)
			JsonTableJoinReset(state->u.scan.nested);
	}
}

/*
 * Fetch next row from a simple scan with outer/inner joined nested subscans.
 *
 * Returned false at the end of a scan, true otherwise.
 */
static bool
JsonTableNextRow(JsonTableScanState *scan)
{
	/* reset context item if requested */
	if (scan->reset)
	{
		JsonTableResetContextItem(scan, scan->parent->current);
		scan->reset = false;
	}

	if (scan->advanceNested)
	{
		/* fetch next nested row */
		scan->advanceNested = JsonTableNextJoinRow(scan->nested);

		if (scan->advanceNested)
			return true;
	}

	for (;;)
	{
		/* fetch next row */
		JsonbValue *jbv = JsonValueListNext(&scan->found, &scan->iter);
		MemoryContext oldcxt;

		if (!jbv)
		{
			scan->current = PointerGetDatum(NULL);
			return false;	/* end of scan */
		}

		/* set current row item */
		oldcxt = MemoryContextSwitchTo(scan->mcxt);
		scan->current = JsonbPGetDatum(JsonbValueToJsonb(jbv));
		MemoryContextSwitchTo(oldcxt);

		scan->ordinal++;

		if (!scan->nested)
			break;

		JsonTableJoinReset(scan->nested);

		scan->advanceNested = JsonTableNextJoinRow(scan->nested);

		if (scan->advanceNested || scan->outerJoin)
			break;

		/* state->ordinal--; */	/* skip current outer row, reset counter */
	}

	return true;
}

/*
 * JsonTableFetchRow
 *		Prepare the next "current" tuple for upcoming GetValue calls.
 *		Returns FALSE if the row-filter expression returned no more rows.
 */
static bool
JsonTableFetchRow(TableFuncScanState *state)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableFetchRow");

	if (cxt->empty)
		return false;

	return JsonTableNextRow(&cxt->root);
}

/*
 * JsonTableGetValue
 *		Return the value for column number 'colnum' for the current row.
 *
 * This leaks memory, so be sure to reset often the context in which it's
 * called.
 */
static Datum
JsonTableGetValue(TableFuncScanState *state, int colnum,
				  Oid typid, int32 typmod, bool *isnull)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableGetValue");
	ExprContext *econtext = state->ss.ps.ps_ExprContext;
	ExprState  *estate = cxt->colexprs[colnum].expr;
	JsonTableScanState *scan = cxt->colexprs[colnum].scan;
	Datum		result;

	if (!DatumGetPointer(scan->current)) /* NULL from outer/union join */
	{
		result = (Datum) 0;
		*isnull = true;
	}
	else if (estate)	/* regular column */
	{
		result = ExecEvalExprPassingCaseValue(estate, econtext, isnull,
											  scan->current, false);
	}
	else
	{
		result = Int32GetDatum(scan->ordinal);	/* ordinality column */
		*isnull = false;
	}

	return result;
}

/*
 * JsonTableDestroyOpaque
 */
static void
JsonTableDestroyOpaque(TableFuncScanState *state)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableDestroyOpaque");

	/* not valid anymore */
	cxt->magic = 0;

	state->opaque = NULL;
}

const TableFuncRoutine JsonbTableRoutine =
{
	JsonTableInitOpaque,
	JsonTableSetDocument,
	NULL,
	NULL,
	NULL,
	JsonTableFetchRow,
	JsonTableGetValue,
	JsonTableDestroyOpaque
};
