#include "postgres.h"

#include <time.h>
#include <unistd.h>

#include "access/relation.h"
#include "catalog/objectaddress.h"
#include "fmgr.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#define HNSW_GRAPH_FILE "hnsw_graph.json"

/*
 * Algorithm 5 from paper
 */
static List *
get_scan_item_with_trace(char *base, Relation index, Datum q, int ef, HnswTraceInfo *trace)
{
	FmgrInfo   *procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	Oid			collation = index->rd_indcollation[0];
	HnswElement entryPoint;
	int			m;
	List	   *ep;
	List	   *w;

	/* Get m and entry point */
	HnswGetMetaPageInfo(index, &m, &entryPoint);

	if (entryPoint == NULL)
		return NIL;

	ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, false));

	for (int lc = entryPoint->level; lc >= 1; lc--)
	{
		w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL, trace);
		ep = w;
	}

	return HnswSearchLayer(base, q, ep, ef, 0, index, procinfo, collation, m, false, NULL, trace);
}

static bool
has_list_member(char *base, List *list, HnswElement target)
{
	ListCell *lc;

	foreach(lc, list)
	{
		HnswTraceNodeInfo *n = (HnswTraceNodeInfo *) lfirst(lc);

		if (n->blkno == target->blkno && n->offno == target->offno)
			return true;
	}

	return false;
}

HnswTraceInfo *HnswInitTraceInfo(void)
{
	HnswTraceInfo *trace = palloc(sizeof(HnswTraceInfo));
	trace->nodes = NIL;
	trace->edges = NIL;
	return trace;
}

static HnswTraceNodeInfo *
make_trace_node(HnswElement he, int lc, float distance)
{
	HnswTraceNodeInfo *node = palloc(sizeof(HnswTraceNodeInfo));
	node->level = (uint8) lc;
	node->blkno = he->blkno;
	node->offno = he->offno;
	node->distance = distance;
	return node;
}

static HnswTraceEdgeInfo *
make_trace_edge(HnswElement src, HnswElement dst)
{
	HnswTraceEdgeInfo *edge = palloc(sizeof(HnswTraceEdgeInfo));
	edge->srcBlkno = src->blkno;
	edge->srcOffno = src->offno;
	edge->dstBlkno = dst->blkno;
	edge->dstOffno = dst->offno;
	return edge;
}

void
HnswAddTraceEdge(char *base, HnswCandidate *src, HnswCandidate *dst, int lc, HnswTraceInfo *trace)
{
	HnswElement se = HnswPtrAccess(base, src->element);
	HnswElement de = HnswPtrAccess(base, dst->element);

	if (!has_list_member(base, trace->nodes, se))
		trace->nodes = lappend(trace->nodes, make_trace_node(se, lc, src->distance));

	if (!has_list_member(base, trace->nodes, de))
		trace->nodes = lappend(trace->nodes, make_trace_node(de, lc, dst->distance));

	trace->edges = lappend(trace->edges, make_trace_edge(se, de));
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_visualize);
Datum
hnsw_visualize(PG_FUNCTION_ARGS)
{
	Oid				relOid;
	Datum			q;
	int32			ef;
	Relation		index;
	ForkNumber		forkNumber;
	char		   *forkString;
	AclResult		aclresult;
	List		   *w;
	FILE		   *file;
	int				ret;
	char			transient_dump_file_path[MAXPGPATH];
	char		   *base = NULL;
	HnswTraceInfo  *trace;
	const char	   *json_eos[2] = { "", "," };

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation cannot be null")));

	relOid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("query data cannot be null")));

	q = PG_GETARG_DATUM(1);

	ef = PG_GETARG_INT32(2);
	if (ef < HNSW_MIN_EF_CONSTRUCTION || ef >= HNSW_MAX_EF_CONSTRUCTION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ef must be between %d and %d",
						HNSW_MIN_EF_CONSTRUCTION, HNSW_MAX_EF_CONSTRUCTION)));

	/* TODO: Need to pass fork name as argument. */
	forkString = "main";
	forkNumber = forkname_to_number(forkString);

	/* Open relation and check privileges. */
	index = relation_open(relOid, AccessShareLock);
	aclresult = pg_class_aclcheck(relOid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, get_relkind_objtype(index->rd_rel->relkind), get_rel_name(relOid));

	/* Check that the fork exists. */
	if (!smgrexists(RelationGetSmgr(index), forkNumber))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("fork \"%s\" does not exist for this relation",
						forkString)));

	trace = HnswInitTraceInfo();

	w = get_scan_item_with_trace(base, index, q, ef, trace);

	if (list_length(w) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no elements found")));
	}

	snprintf(transient_dump_file_path, MAXPGPATH, "%s.tmp", HNSW_GRAPH_FILE);
	file = AllocateFile(transient_dump_file_path, "w");
	if (!file)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						transient_dump_file_path)));

	fprintf(file, "{\n\"nodes\": [\n");

	while (list_length(trace->nodes) > 0)
	{
		HnswTraceNodeInfo *node = (HnswTraceNodeInfo *) linitial(trace->nodes);
		trace->nodes = list_delete_first(trace->nodes);

		ret = fprintf(file, "{\"blkno\": %d, \"offno\": %d, \"level\": %d, \"distance\": %f}%s\n",
				node->blkno, node->offno, node->level, node->distance,
				json_eos[list_length(trace->nodes) > 0]);
		if (ret < 0)
		{
			int			save_errno = errno;

			FreeFile(file);
			unlink(transient_dump_file_path);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m",
							transient_dump_file_path)));
		}
	}

	fprintf(file, "],\n\"edges\": [\n");

	while (list_length(trace->edges) > 0)
	{
		HnswTraceEdgeInfo *edge = (HnswTraceEdgeInfo *) linitial(trace->edges);
		trace->edges = list_delete_first(trace->edges);

		ret = fprintf(file, "{\"src_blkno\": %d, \"src_offno\": %d, \"dst_blkno\": %d, \"dst_offno\": %d}%s\n",
				edge->srcBlkno, edge->srcOffno, edge->dstBlkno, edge->dstOffno,
				json_eos[list_length(trace->edges) > 0]);
		if (ret < 0)
		{
			int			save_errno = errno;

			FreeFile(file);
			unlink(transient_dump_file_path);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m",
							transient_dump_file_path)));
		}
	}

	fprintf(file, "]\n}\n");

	/* Close relation, release lock. */
	relation_close(index, AccessShareLock);

	/*
	 * Rename transient_dump_file_path to HNSW_GRAPH_FILE to make things
	 * permanent.
	 */
	ret = FreeFile(file);
	if (ret != 0)
	{
		int	save_errno = errno;

		unlink(transient_dump_file_path);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						transient_dump_file_path)));
	}

	(void) durable_rename(transient_dump_file_path, HNSW_GRAPH_FILE, ERROR);

	PG_RETURN_BOOL(true);
}