/*-------------------------------------------------------------------------
 *
 * jsonpath.h
 *	Definitions of jsonpath datatype
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/include/utils/jsonpath.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_H
#define JSONPATH_H

#include "fmgr.h"
#include "executor/tablefunc.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/jsonb.h"

typedef struct
{
	int32	vl_len_;/* varlena header (do not touch directly!) */
	uint32	header;	/* just version, other bits are reservedfor future use */
	uint32	ext_items_count; /* number of items that need cache for external execution */
	char	data[FLEXIBLE_ARRAY_MEMBER];
} JsonPath;

#define JSONPATH_VERSION	(0x01)
#define JSONPATH_LAX		(0x80000000)
#define JSONPATH_HDRSZ		(offsetof(JsonPath, data))

/* flags for JsonPathItem */
#define JSPI_OUT_PATH		0x01

#define DatumGetJsonPathP(d)			((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetJsonPathPCopy(d)		((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_JSONPATH_P(x)			DatumGetJsonPathP(PG_GETARG_DATUM(x))
#define PG_GETARG_JSONPATH_P_COPY(x)	DatumGetJsonPathPCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONPATH_P(p)			PG_RETURN_POINTER(p)

#define jspIsScalar(type) ((type) >= jpiNull && (type) <= jpiBool)

/*
 * All node's type of jsonpath expression
 */
typedef enum JsonPathItemType {
		jpiNull = jbvNull,
		jpiString = jbvString,
		jpiNumeric = jbvNumeric,
		jpiBool = jbvBool,
		jpiAnd,
		jpiOr,
		jpiNot,
		jpiIsUnknown,
		jpiEqual,
		jpiNotEqual,
		jpiLess,
		jpiGreater,
		jpiLessOrEqual,
		jpiGreaterOrEqual,
		jpiAdd,
		jpiSub,
		jpiMul,
		jpiDiv,
		jpiMod,
		jpiPlus,
		jpiMinus,
		jpiAnyArray,
		jpiAnyKey,
		jpiIndexArray,
		jpiAny,
		jpiKey,
		jpiCurrent,
		jpiCurrentN,
		jpiRoot,
		jpiVariable,
		jpiFilter,
		jpiExists,
		jpiType,
		jpiSize,
		jpiAbs,
		jpiFloor,
		jpiCeiling,
		jpiDouble,
		jpiDatetime,
		jpiKeyValue,
		jpiSubscript,
		jpiLast,
		jpiStartsWith,
		jpiLikeRegex,
		jpiSequence,
		jpiArray,
		jpiObject,
		jpiObjectField,

		jpiBinary = 0xFF /* for jsonpath operators implementation only */
} JsonPathItemType;

/* XQuery regex mode flags for LIKE_REGEX predicate */
#define JSP_REGEX_ICASE		0x01	/* i flag, case insensitive */
#define JSP_REGEX_SLINE		0x02	/* s flag, single-line mode */
#define JSP_REGEX_MLINE		0x04	/* m flag, multi-line mode */
#define JSP_REGEX_WSPACE	0x08	/* x flag, expanded syntax */

#define jspIsBooleanOp(type) ( \
	(type) == jpiAnd || \
	(type) == jpiOr || \
	(type) == jpiNot || \
	(type) == jpiIsUnknown || \
	(type) == jpiEqual || \
	(type) == jpiNotEqual || \
	(type) == jpiLess || \
	(type) == jpiGreater || \
	(type) == jpiLessOrEqual || \
	(type) == jpiGreaterOrEqual || \
	(type) == jpiExists || \
	(type) == jpiStartsWith \
)

/*
 * Support functions to parse/construct binary value.
 * Unlike many other representation of expression the first/main
 * node is not an operation but left operand of expression. That
 * allows to implement cheep follow-path descending in jsonb
 * structure and then execute operator with right operand
 */

typedef struct JsonPathItem {
	JsonPathItemType	type;
	uint8				flags;

	/* position form base to next node */
	int32			nextPos;

	/*
	 * pointer into JsonPath value to current node, all
	 * positions of current are relative to this base
	 */
	char			*base;

	union {
		/* classic operator with two operands: and, or etc */
		struct {
			int32	left;
			int32	right;
		} args;

		/* any unary operation */
		int32		arg;

		/* storage for jpiIndexArray: indexes of array */
		struct {
			int32	nelems;
			struct {
				int32	from;
				int32	to;
			}	   *elems;
		} array;

		/* jpiAny: levels */
		struct {
			uint32	first;
			uint32	last;
		} anybounds;

		struct {
			int32	nelems;
			int32  *elems;
		} sequence;

		struct {
			int32	nfields;
			struct {
				int32	key;
				int32	val;
			}	   *fields;
		} object;

		struct {
			int32		level;
		} current;

		struct {
			char		*data;  /* for bool, numeric and string/key */
			int32		datalen; /* filled only for string/key */
		} value;

		struct {
			int32		expr;
			char		*pattern;
			int32		patternlen;
			uint32		flags;
		} like_regex;
	} content;
} JsonPathItem;

#define jspHasNext(jsp) ((jsp)->nextPos > 0)
#define jspOutPath(jsp) (((jsp)->flags & JSPI_OUT_PATH) != 0)

extern void jspInit(JsonPathItem *v, JsonPath *js);
extern void jspInitByBuffer(JsonPathItem *v, char *base, int32 pos);
extern bool jspGetNext(JsonPathItem *v, JsonPathItem *a);
extern void jspGetArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetLeftArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetRightArg(JsonPathItem *v, JsonPathItem *a);
extern Numeric	jspGetNumeric(JsonPathItem *v);
extern bool		jspGetBool(JsonPathItem *v);
extern char * jspGetString(JsonPathItem *v, int32 *len);
extern bool jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from,
								 JsonPathItem *to, int i);
extern void jspGetSequenceElement(JsonPathItem *v, int i, JsonPathItem *elem);
extern void jspGetObjectField(JsonPathItem *v, int i,
							  JsonPathItem *key, JsonPathItem *val);

/*
 * Parsing
 */

typedef struct JsonPathParseItem JsonPathParseItem;

struct JsonPathParseItem {
	JsonPathItemType	type;
	uint8				flags;
	JsonPathParseItem	*next; /* next in path */

	union {

		/* classic operator with two operands: and, or etc */
		struct {
			JsonPathParseItem	*left;
			JsonPathParseItem	*right;
		} args;

		/* any unary operation */
		JsonPathParseItem	*arg;

		/* storage for jpiIndexArray: indexes of array */
		struct {
			int		nelems;
			struct JsonPathParseArraySubscript
			{
				JsonPathParseItem *from;
				JsonPathParseItem *to;
			}	   *elems;
		} array;

		/* jpiAny: levels */
		struct {
			uint32	first;
			uint32	last;
		} anybounds;

		struct {
			JsonPathParseItem *expr;
			char	*pattern; /* could not be not null-terminated */
			uint32	patternlen;
			uint32	flags;
		} like_regex;

		struct {
			List   *elems;
		} sequence;

		struct {
			List   *fields;
		} object;

		struct {
			int		level;
		} current;

		JsonPath   *binary;

		/* scalars */
		Numeric		numeric;
		bool		boolean;
		struct {
			uint32	len;
			char	*val; /* could not be not null-terminated */
		} string;
	} value;
};

typedef struct JsonPathParseResult
{
	JsonPathParseItem *expr;
	bool		lax;
} JsonPathParseResult;

extern JsonPathParseResult* parsejsonpath(const char *str, int len);

/*
 * Evaluation of jsonpath
 */

typedef enum JsonPathExecStatus
{
	jperOk = 0,
	jperError,
	jperFatalError,
	jperNotFound
} JsonPathExecStatus;

typedef uint64 JsonPathExecResult;

#define jperStatus(jper)	((JsonPathExecStatus)(uint32)(jper))
#define jperIsError(jper)	(jperStatus(jper) == jperError)
#define jperGetError(jper)	((uint32)((jper) >> 32))
#define jperMakeError(err)	(((uint64)(err) << 32) | jperError)

typedef Datum (*JsonPathVariable_cb)(void *, bool *);

typedef struct JsonPathVariable	{
	text					*varName;
	Oid						typid;
	int32					typmod;
	JsonPathVariable_cb		cb;
	void					*cb_arg;
} JsonPathVariable;

typedef struct JsonPathVariableEvalContext
{
	JsonPathVariable var;
	struct ExprContext *econtext;
	struct ExprState  *estate;
	MemoryContext mcxt;		/* memory context for cached value */
	Datum		value;
	bool		isnull;
	bool		evaluated;
} JsonPathVariableEvalContext;

typedef struct JsonValueList
{
	JsonbValue *singleton;
	List	   *list;
} JsonValueList;

typedef struct JsonItemStackEntry
{
	JsonbValue *item;
	struct JsonItemStackEntry *parent;
} JsonItemStackEntry;

typedef JsonItemStackEntry *JsonItemStack;

typedef struct JsonPathExecContext
{
	List	   *vars;
	bool		lax;
	JsonbValue *root;				/* for $ evaluation */
	JsonItemStack stack;			/* for @N evaluation */
	void	  **cache;
	MemoryContext cache_mcxt;
	int			innermostArraySize;	/* for LAST array index evaluation */
} JsonPathExecContext;

typedef struct JsonValueListIterator
{
	ListCell   *lcell;
} JsonValueListIterator;

JsonPathExecResult	executeJsonPath(JsonPath *path,
									List	*vars, /* list of JsonPathVariable */
									Jsonb *json,
									JsonValueList *foundJson,
									void **pCache,
									MemoryContext cacheCxt);

extern bool  JsonbPathExists(Datum jb, JsonPath *path, List *vars);
extern Datum JsonbPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper,
			   bool *empty, List *vars);
extern JsonbValue *JsonbPathValue(Datum jb, JsonPath *jp, bool *empty,
			   List *vars);

extern bool JsonPathExists(Datum json, JsonPath *path, List *vars);
extern JsonbValue *JsonPathValue(Datum json, JsonPath *jp, bool *empty,
			  List *vars);
extern Datum JsonPathQuery(Datum json, JsonPath *jp, JsonWrapper wrapper,
			  bool *empty, List *vars);

extern Datum EvalJsonPathVar(void *cxt, bool *isnull);

extern const TableFuncRoutine JsonTableRoutine;
extern const TableFuncRoutine JsonbTableRoutine;


/*
 * Returns jbv* type of of JsonbValue. Note, it never returns
 * jbvBinary as is - jbvBinary is used as mark of store naked
 * scalar value. To improve readability it defines jbvScalar
 * as alias to jbvBinary
 */
#define jbvScalar jbvBinary

static inline int
JsonbType(JsonbValue *jb)
{
	int type = jb->type;

	if (jb->type == jbvBinary)
	{
		JsonbContainer	*jbc = (void *) jb->val.binary.data;

		if (JsonContainerIsScalar(jbc))
			type = jbvScalar;
		else if (JsonContainerIsObject(jbc))
			type = jbvObject;
		else if (JsonContainerIsArray(jbc))
			type = jbvArray;
		else
			elog(ERROR, "Unknown container type: 0x%08x", jbc->header);
	}

	return type;
}

extern int JsonbArraySize(JsonbValue *jb);

static inline JsonbValue *
copyJsonbValue(JsonbValue *src)
{
	JsonbValue *dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
}

extern JsonbValue *JsonbWrapItemInArray(JsonbValue *jbv);
extern JsonbValue *JsonbWrapItemsInArray(const JsonValueList *items);

static inline void
pushJsonItem(JsonItemStack *stack, JsonItemStackEntry *entry, JsonbValue *item)
{
	entry->item = item;
	entry->parent = *stack;
	*stack = entry;
}

static inline void
popJsonItem(JsonItemStack *stack)
{
	*stack = (*stack)->parent;
}

static inline void
JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv)
{
	if (jvl->singleton)
	{
		jvl->list = list_make2(jvl->singleton, jbv);
		jvl->singleton = NULL;
	}
	else if (!jvl->list)
		jvl->singleton = jbv;
	else
		jvl->list = lappend(jvl->list, jbv);
}

static inline int
JsonValueListLength(const JsonValueList *jvl)
{
	return jvl->singleton ? 1 : list_length(jvl->list);
}

static inline bool
JsonValueListIsEmpty(JsonValueList *jvl)
{
	return !jvl->singleton && list_length(jvl->list) <= 0;
}

static inline JsonbValue *
JsonValueListHead(JsonValueList *jvl)
{
	return jvl->singleton ? jvl->singleton : linitial(jvl->list);
}

static inline List *
JsonValueListGetList(JsonValueList *jvl)
{
	if (jvl->singleton)
		return list_make1(jvl->singleton);

	return jvl->list;
}

#define JsonValueListIteratorEnd ((ListCell *) -1)

/*
 * Get the next item from the sequence advancing iterator.
 */
static inline JsonbValue *
JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)
{
	if (it->lcell == JsonValueListIteratorEnd)
		return NULL;

	if (it->lcell)
		it->lcell = lnext(it->lcell);
	else
	{
		if (jvl->singleton)
		{
			it->lcell = JsonValueListIteratorEnd;
			return jvl->singleton;
		}

		it->lcell = list_head(jvl->list);
	}

	if (!it->lcell)
	{
		it->lcell = JsonValueListIteratorEnd;
		return NULL;
	}

	return lfirst(it->lcell);
}

extern void JsonValueListConcat(JsonValueList *jvl1, JsonValueList jvl2);

extern JsonPathExecResult jspRecursiveExecute(JsonPathExecContext *cxt,
					JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
extern JsonPathExecResult jspRecursiveExecuteNested(JsonPathExecContext *cxt,
						  JsonPathItem *jsp, JsonbValue *jb,
						  JsonValueList *found);
extern JsonPathExecResult jspCompareItems(int32 op, JsonbValue *jb1,
				JsonbValue *jb2);

#endif
