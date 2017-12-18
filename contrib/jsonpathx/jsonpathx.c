/*-------------------------------------------------------------------------
 *
 * jsonpathx.c
 *	   Extended jsonpath item methods and operators for jsonb type.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	   contrib/jsonpathx/jsonpathx.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/jsonpath.h"

#ifdef JSONPATHX_JSON_C
# define JSONXOID JSONOID
# define JSONX json
#else
# define JSONXOID JSONBOID
# define JSONX jsonb

PG_MODULE_MAGIC;
#endif

#define CppConcat2(x, y)	CppConcat(x, y)

#define JsonpathxFunc(fn) \
	PG_FUNCTION_INFO_V1(CppConcat2(CppConcat2(fn, _), JSONX)); \
	Datum \
	CppConcat2(CppConcat2(fn, _), JSONX)(PG_FUNCTION_ARGS)

static JsonPathExecResult
jspRecursiveExecuteSingleton(JsonPathExecContext *cxt, JsonPathItem *jsp,
							 JsonbValue *jb, JsonbValue **result)
{
	JsonValueList reslist = { 0 };
	JsonPathExecResult res = jspRecursiveExecute(cxt, jsp, jb, &reslist);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&reslist) != 1)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	*result = JsonValueListHead(&reslist);

	return jperOk;
}

static JsonPathExecResult
jspMap(JsonPathFuncContext *fcxt, bool flat)
{
	JsonPathExecContext *cxt = fcxt->cxt;
	JsonbValue *jb = fcxt->item;
	JsonPathItem *func = &fcxt->args[jb ? 0 : 1];
	void	   **funccache = &fcxt->argscache[jb ? 0 : 1];
	JsonPathExecResult res;
	JsonbValue *args[3];
	JsonbValue	jbvidx;
	int			index = 0;
	int			nargs = 1;

	if (fcxt->nargs != (jb ? 1 : 2))
		return jperMakeError(ERRCODE_JSON_SCALAR_REQUIRED); /* FIXME */

	if (func->type == jpiLambda && func->content.lambda.nparams > 1)
	{
		args[nargs++] = &jbvidx;
		jbvidx.type = jbvNumeric;
	}

	if (!jb)
	{
		JsonValueList items = { 0 };
		JsonValueListIterator iter = { 0 };
		JsonbValue *item;

		res = jspRecursiveExecute(cxt, &fcxt->args[0], fcxt->jb, &items);

		if (jperIsError(res))
			return res;

		while ((item = JsonValueListNext(&items, &iter)))
		{
			JsonValueList reslist = { 0 };

			args[0] = item;

			if (nargs > 1)
			{
				jbvidx.val.numeric = DatumGetNumeric(
					DirectFunctionCall1(int4_numeric, Int32GetDatum(index)));
				index++;
			}

			res = jspRecursiveExecuteLambda(cxt, func, fcxt->jb, &reslist,
											args, nargs, funccache);

			if (jperIsError(res))
				return res;

			if (flat)
			{
				JsonValueListConcat(fcxt->result, reslist);
			}
			else
			{
				if (JsonValueListLength(&reslist) != 1)
					return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

				JsonValueListAppend(fcxt->result, JsonValueListHead(&reslist));
			}
		}
	}
	else if (JsonbType(jb) != jbvArray)
	{
		JsonValueList reslist = { 0 };
		JsonItemStackEntry entry;

		if (!cxt->lax)
			return jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);

		args[0] = jb;

		if (nargs > 1)
			jbvidx.val.numeric = DatumGetNumeric(
				DirectFunctionCall1(int4_numeric, Int32GetDatum(0)));

		/* push additional stack entry for the whole item */
		pushJsonItem(&cxt->stack, &entry, jb);
		res = jspRecursiveExecuteLambda(cxt, func, jb, &reslist, args, nargs,
										funccache);
		popJsonItem(&cxt->stack);

		if (jperIsError(res))
			return res;

		if (flat)
		{
			JsonValueListConcat(fcxt->result, reslist);
		}
		else
		{
			if (JsonValueListLength(&reslist) != 1)
				return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

			JsonValueListAppend(fcxt->result, JsonValueListHead(&reslist));
		}
	}
	else
	{
		JsonbValue	elembuf;
		JsonbValue *elem;
		JsonbIterator *it = NULL;
		JsonbIteratorToken tok;
		JsonValueList result = { 0 };
		JsonItemStackEntry entry;
		int			size = JsonbArraySize(jb);
		int			i;

		if (jb->type == jbvBinary && size > 0)
		{
			elem = &elembuf;
			it = JsonbIteratorInit(jb->val.binary.data);
			tok = JsonbIteratorNext(&it, &elembuf, false);
			if (tok != WJB_BEGIN_ARRAY)
				elog(ERROR, "unexpected jsonb token at the array start");
		}

		/* push additional stack entry for the whole array */
		pushJsonItem(&cxt->stack, &entry, jb);

		if (nargs > 1)
		{
			nargs = 3;
			args[2] = jb;
		}

		for (i = 0; i < size; i++)
		{
			JsonValueList reslist = { 0 };

			if (it)
			{
				tok = JsonbIteratorNext(&it, elem, true);
				if (tok != WJB_ELEM)
					break;
			}
			else
				elem = &jb->val.array.elems[i];

			args[0] = elem;

			if (nargs > 1)
			{
				jbvidx.val.numeric = DatumGetNumeric(
					DirectFunctionCall1(int4_numeric, Int32GetDatum(index)));
				index++;
			}

			res = jspRecursiveExecuteLambda(cxt, func, jb, &reslist,
											args, nargs, funccache);

			if (jperIsError(res))
			{
				popJsonItem(&cxt->stack);
				return res;
			}

			if (JsonValueListLength(&reslist) != 1)
			{
				popJsonItem(&cxt->stack);
				return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);
			}

			if (flat)
			{
				JsonbValue *jbvarr = JsonValueListHead(&reslist);

				if (JsonbType(jbvarr) == jbvArray)
				{
					Jsonb *jbarr = JsonbValueToJsonb(jbvarr);
					JsonbValue elem;
					JsonbIterator *it = JsonbIteratorInit(&jbarr->root);
					JsonbIteratorToken tok;

					while ((tok = JsonbIteratorNext(&it, &elem, true)) != WJB_DONE)
					{
						if (tok == WJB_ELEM)
							JsonValueListAppend(&result, copyJsonbValue(&elem));
					}
				}
				else if (cxt->lax)
				{
					JsonValueListConcat(&result, reslist);
				}
				else
				{
					popJsonItem(&cxt->stack);
					return jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
				}
			}
			else
			{
				JsonValueListConcat(&result, reslist);
			}
		}

		popJsonItem(&cxt->stack);

		JsonValueListAppend(fcxt->result, JsonbWrapItemsInArray(&result));
	}

	return jperOk;
}

JsonpathxFunc(jsonpath_map)
{
	return Int64GetDatum(jspMap((void *) PG_GETARG_POINTER(0), false));
}

JsonpathxFunc(jsonpath_flatmap)
{
	return Int64GetDatum(jspMap((void *) PG_GETARG_POINTER(0), true));
}

typedef enum FoldType { FOLD_REDUCE, FOLD_LEFT, FOLD_RIGHT } FoldType;

typedef struct FoldContext
{
	JsonPathExecContext *cxt;
	JsonPathItem *func;
	void	  **pfunccache;
	JsonbValue *item;
	JsonbValue *result;
	JsonbValue *args[4];
	JsonbValue **argres;
	JsonbValue **argelem;
	JsonbValue	argidx;
	int			nargs;
} FoldContext;

static void
foldInit(FoldContext *fcxt, JsonPathExecContext *cxt, JsonPathItem *func,
		 void **pfunccache, JsonbValue *array, JsonbValue *item,
		 JsonbValue *result, FoldType foldtype)
{
	fcxt->cxt = cxt;
	fcxt->func = func;
	fcxt->pfunccache = pfunccache;
	fcxt->item = item;
	fcxt->result = result;

	if (foldtype == FOLD_RIGHT)
	{
		/* swap args for foldr() */
		fcxt->argres = &fcxt->args[1];
		fcxt->argelem = &fcxt->args[0];
	}
	else
	{
		fcxt->argres = &fcxt->args[0];
		fcxt->argelem = &fcxt->args[1];
	}

	fcxt->nargs = 2;

	if (func->type == jpiLambda && func->content.lambda.nparams > 2)
	{
		fcxt->args[fcxt->nargs++] = &fcxt->argidx;
		fcxt->argidx.type = jbvNumeric;
		if (array)
			fcxt->args[fcxt->nargs++] = array;
	}
}

static JsonPathExecResult
foldAccumulate(FoldContext *fcxt, JsonbValue *element, int index)
{
	JsonValueList reslist = { 0 };
	JsonPathExecResult res;

	if (!fcxt->result) /* first element of reduce */
	{
		fcxt->result = element;
		return jperOk;
	}

	*fcxt->argres = fcxt->result;
	*fcxt->argelem = element;

	if (fcxt->nargs > 2)
		fcxt->argidx.val.numeric = DatumGetNumeric(
			DirectFunctionCall1(int4_numeric, Int32GetDatum(index)));

	res = jspRecursiveExecuteLambda(fcxt->cxt, fcxt->func, fcxt->item, &reslist,
									fcxt->args, fcxt->nargs, fcxt->pfunccache);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&reslist) != 1)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	fcxt->result = JsonValueListHead(&reslist);

	return jperOk;
}

static JsonbValue *
foldDone(FoldContext *fcxt)
{
	return fcxt->result;
}

static void
list_reverse(List *itemlist)
{
	ListCell   *curlc;
	ListCell   *prevlc = NULL;

	if (list_length(itemlist) <= 1)
		return;

	curlc = itemlist->head;
	itemlist->head = itemlist->tail;
	itemlist->tail = curlc;

	while (curlc)
	{
		ListCell *next = curlc->next;

		curlc->next = prevlc;
		prevlc = curlc;
		curlc = next;
	}
}

static JsonPathExecResult
jspFoldSeq(JsonPathFuncContext *fcxt, FoldType ftype)
{
	FoldContext foldcxt;
	JsonValueList items = { 0 };
	JsonbValue *result = NULL;
	JsonPathExecResult res;
	int			size;

	res = jspRecursiveExecute(fcxt->cxt, &fcxt->args[0], fcxt->jb, &items);

	if (jperIsError(res))
		return res;

	size = JsonValueListLength(&items);

	if (ftype == FOLD_REDUCE)
	{
		if (!size)
			return jperNotFound;

		if (size == 1)
		{
			JsonValueListAppend(fcxt->result, JsonValueListHead(&items));
			return jperOk;
		}
	}
	else
	{
		res = jspRecursiveExecuteSingleton(fcxt->cxt, &fcxt->args[2], fcxt->jb,
										   &result);
		if (jperIsError(res))
			return res;

		if (!size)
		{
			JsonValueListAppend(fcxt->result, result);
			return jperOk;
		}
	}

	foldInit(&foldcxt, fcxt->cxt, &fcxt->args[1], &fcxt->argscache[1], NULL,
			 fcxt->jb, result, ftype);

	if (ftype == FOLD_RIGHT)
	{
		List	   *itemlist = JsonValueListGetList(&items);
		ListCell   *lc;
		int			index = list_length(itemlist) - 1;

		list_reverse(itemlist);

		foreach(lc, itemlist)
		{
			res = foldAccumulate(&foldcxt, lfirst(lc), index--);

			if (jperIsError(res))
			{
				(void) foldDone(&foldcxt);
				return res;
			}
		}
	}
	else
	{
		JsonValueListIterator iter = { 0 };
		JsonbValue *item;
		int			index = 0;

		while ((item = JsonValueListNext(&items, &iter)))
		{
			res = foldAccumulate(&foldcxt, item, index++);

			if (jperIsError(res))
			{
				(void) foldDone(&foldcxt);
				return res;
			}
		}
	}

	result = foldDone(&foldcxt);

	JsonValueListAppend(fcxt->result, result);

	return jperOk;
}

static JsonPathExecResult
jspFoldArray(JsonPathFuncContext *fcxt, FoldType ftype, JsonbValue *item)
{
	JsonbValue *result = NULL;
	JsonPathExecResult res;
	int			size;

	if (JsonbType(item) != jbvArray)
	{
		if (!fcxt->cxt->lax)
			return jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);

		if (ftype == FOLD_REDUCE)
		{
			JsonValueListAppend(fcxt->result, item);

			return jperOk;
		}

		item = JsonbWrapItemInArray(item);
	}

	size = JsonbArraySize(item);

	if (ftype == FOLD_REDUCE)
	{
		if (!size)
			return jperNotFound;
	}
	else
	{
		res = jspRecursiveExecuteSingleton(fcxt->cxt, &fcxt->args[1], fcxt->jb,
										   &result);
		if (jperIsError(res))
			return res;
	}

	if (ftype == FOLD_REDUCE && size == 1)
	{
		if (item->type == jbvBinary)
		{
			result = getIthJsonbValueFromContainer(item->val.binary.data, 0);
			if (!result)
				return jperNotFound;
		}
		else
		{
			Assert(item->type == jbvArray);
			result = copyJsonbValue(&item->val.array.elems[0]);
		}
	}
	else if (size)
	{
		FoldContext foldcxt;
		JsonbIterator *it = NULL;
		JsonbIteratorToken tok;
		JsonbValue	elembuf;
		JsonbValue *elem;
		int			i;
		bool		foldr = ftype == FOLD_RIGHT;

		if (item->type == jbvBinary)
		{
			if (foldr)
			{
				/* unpack array for reverse iteration */
				JsonbParseState *ps = NULL;

				item = pushJsonbValue(&ps, WJB_ELEM, item);
			}
			else
			{
				elem = &elembuf;
				it = JsonbIteratorInit(item->val.binary.data);
				tok = JsonbIteratorNext(&it, elem, false);
				if (tok != WJB_BEGIN_ARRAY)
					elog(ERROR, "unexpected jsonb token at the array start");
			}
		}

		foldInit(&foldcxt, fcxt->cxt, &fcxt->args[0], &fcxt->argscache[0],
				 item, fcxt->jb, result, ftype);

		for (i = 0; i < size; i++)
		{
			JsonbValue *el;
			int			index;

			if (it)
			{
				tok = JsonbIteratorNext(&it, elem, true);
				if (tok != WJB_ELEM)
					break;
				index = i;
			}
			else
			{
				index = foldr ? size - i - 1 : i;
				elem = &item->val.array.elems[index];
			}

			el = elem;

			if (!i && ftype == FOLD_REDUCE)
				el = copyJsonbValue(el);

			res = foldAccumulate(&foldcxt, el, index);

			if (jperIsError(res))
			{
				(void) foldDone(&foldcxt);
				return res;
			}
		}

		result = foldDone(&foldcxt);
	}

	JsonValueListAppend(fcxt->result, result);

	return jperOk;
}

static JsonPathExecResult
jspFold(JsonPathFuncContext *fcxt, FoldType ftype)
{
	JsonbValue *item = fcxt->item;

	if (fcxt->nargs != (ftype == FOLD_REDUCE ? 1 : 2) + (item ? 0 : 1))
		return jperMakeError(ERRCODE_JSON_SCALAR_REQUIRED); /* FIXME */

	return item ? jspFoldArray(fcxt, ftype, item) : jspFoldSeq(fcxt, ftype);
}

JsonpathxFunc(jsonpath_reduce)
{
	return Int64GetDatum(jspFold((void *) PG_GETARG_POINTER(0), FOLD_REDUCE));
}

JsonpathxFunc(jsonpath_fold)
{
	return Int64GetDatum(jspFold((void *) PG_GETARG_POINTER(0), FOLD_LEFT));
}

JsonpathxFunc(jsonpath_foldl)
{
	return Int64GetDatum(jspFold((void *) PG_GETARG_POINTER(0), FOLD_LEFT));
}

JsonpathxFunc(jsonpath_foldr)
{
	return Int64GetDatum(jspFold((void *) PG_GETARG_POINTER(0), FOLD_RIGHT));
}

static JsonPathExecResult
jspMinMax(JsonPathFuncContext *fcxt, bool max)
{
	JsonbValue *item = fcxt->item;
	JsonbValue *result = NULL;
	JsonPathItemType cmpop = max ? jpiGreater : jpiLess;

	if (fcxt->nargs != (item ? 0 : 1))
		return jperMakeError(ERRCODE_JSON_SCALAR_REQUIRED); /* FIXME */

	if (!item)
	{
		JsonValueList items = { 0 };
		JsonValueListIterator iter = { 0 };
		JsonbValue *item;
		JsonPathExecResult res;

		res = jspRecursiveExecute(fcxt->cxt, &fcxt->args[0], fcxt->jb, &items);

		if (jperIsError(res))
			return res;

		if (!JsonValueListLength(&items))
			return jperNotFound;

		res = jperOk;

		while ((item = JsonValueListNext(&items, &iter)))
		{
			if (result)
			{
				res = jspCompareItems(cmpop, item, result);

				if (jperIsError(res))
					return jperMakeError(ERRCODE_JSON_SCALAR_REQUIRED);
			}

			if (res == jperOk)
				result = item;
		}
	}
	else if (JsonbType(item) != jbvArray)
	{
		if (!fcxt->cxt->lax)
			return jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);

		result = item;
	}
	else
	{
		JsonbValue	elmebuf;
		JsonbValue *elem;
		JsonbIterator *it = NULL;
		JsonbIteratorToken tok;
		int			size = JsonbArraySize(item);
		int			i;

		if (item->type == jbvBinary)
		{
			elem = &elmebuf;
			it = JsonbIteratorInit(item->val.binary.data);
			tok = JsonbIteratorNext(&it, &elmebuf, false);
			if (tok != WJB_BEGIN_ARRAY)
				elog(ERROR, "unexpected jsonb token at the array start");
		}

		for (i = 0; i < size; i++)
		{
			if (it)
			{
				tok = JsonbIteratorNext(&it, elem, true);
				if (tok != WJB_ELEM)
					break;
			}
			else
				elem = &item->val.array.elems[i];

			if (!i)
			{
				result = it ? copyJsonbValue(elem) : elem;
			}
			else
			{
				JsonPathExecResult res = jspCompareItems(cmpop, elem, result);

				if (jperIsError(res))
					return jperMakeError(ERRCODE_JSON_SCALAR_REQUIRED);

				if (res == jperOk)
					result = it ? copyJsonbValue(elem) : elem;
			}
		}

		if (!result)
			return jperNotFound;
	}

	JsonValueListAppend(fcxt->result, result);
	return jperOk;
}

JsonpathxFunc(jsonpath_min)
{
	return Int64GetDatum(jspMinMax((void *) PG_GETARG_POINTER(0), false));
}

JsonpathxFunc(jsonpath_max)
{
	return Int64GetDatum(jspMinMax((void *) PG_GETARG_POINTER(0), true));
}
