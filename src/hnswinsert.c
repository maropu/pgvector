#include "postgres.h"

#include <math.h>

#include "hnsw.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Get the insert page
 */
static BlockNumber
GetInsertPage(Relation index)
{
	Buffer		buf;
	Page		page;
	HnswMetaPage metap;
	BlockNumber insertPage;

	buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = HnswPageGetMeta(page);

	insertPage = metap->insertPage;

	UnlockReleaseBuffer(buf);

	return insertPage;
}

/*
 * Check for a free offset
 */
static bool
HnswFreeOffset(Relation index, Buffer buf, Page page, HnswElement element, Size ntupSize, Buffer *nbuf, Page *npage, OffsetNumber *freeOffno, OffsetNumber *freeNeighborOffno, BlockNumber *firstFreePage)
{
	OffsetNumber offno;
	OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

	for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		HnswElementTuple etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));

		/* Skip neighbor tuples */
		if (!HnswIsElementTuple(etup))
			continue;

		if (etup->deleted)
		{
			BlockNumber neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
			OffsetNumber neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
			ItemId		itemid;

			if (!BlockNumberIsValid(*firstFreePage))
				*firstFreePage = neighborPage;

			if (neighborPage == BufferGetBlockNumber(buf))
			{
				*nbuf = buf;
				*npage = page;
			}
			else
			{
				*nbuf = ReadBuffer(index, neighborPage);
				LockBuffer(*nbuf, BUFFER_LOCK_EXCLUSIVE);

				/* Skip WAL for now */
				*npage = BufferGetPage(*nbuf);
			}

			itemid = PageGetItemId(*npage, neighborOffno);

			/* Check for space on neighbor tuple page */
			if (PageGetFreeSpace(*npage) + ItemIdGetLength(itemid) - sizeof(ItemIdData) >= ntupSize)
			{
				*freeOffno = offno;
				*freeNeighborOffno = neighborOffno;
				return true;
			}
			else if (*nbuf != buf)
				UnlockReleaseBuffer(*nbuf);
		}
	}

	return false;
}

/*
 * Add a new page
 */
static void
HnswInsertAppendPage(Relation index, Buffer *nbuf, Page *npage, GenericXLogState *state, Page page)
{
	/* Add a new page */
	LockRelationForExtension(index, ExclusiveLock);
	*nbuf = HnswNewBuffer(index, MAIN_FORKNUM);
	UnlockRelationForExtension(index, ExclusiveLock);

	/* Init new page */
	*npage = GenericXLogRegisterBuffer(state, *nbuf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(*nbuf, *npage);

	/* Update previous buffer */
	HnswPageGetOpaque(page)->nextblkno = BufferGetBlockNumber(*nbuf);
}

/*
 * Add to element and neighbor pages
 */
static void
WriteNewElementPages(Relation index, HnswElement e, int m)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		etupSize;
	Size		ntupSize;
	Size		combinedSize;
	HnswElementTuple etup;
	BlockNumber insertPage = GetInsertPage(index);
	BlockNumber originalInsertPage = insertPage;
	int			dimensions = e->vec->dim;
	HnswNeighborTuple ntup;
	Buffer		nbuf;
	Page		npage;
	OffsetNumber freeOffno = InvalidOffsetNumber;
	OffsetNumber freeNeighborOffno = InvalidOffsetNumber;
	BlockNumber firstFreePage = InvalidBlockNumber;

	/* Calculate sizes */
	etupSize = HNSW_ELEMENT_TUPLE_SIZE(dimensions);
	ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);
	combinedSize = etupSize + ntupSize + sizeof(ItemIdData);

	/* Prepare element tuple */
	etup = palloc0(etupSize);
	HnswSetElementTuple(etup, e);

	/* Prepare neighbor tuple */
	ntup = palloc0(ntupSize);
	HnswSetNeighborTuple(ntup, e, m);

	/* Find a page to insert the item */
	for (;;)
	{
		buf = ReadBuffer(index, insertPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		/* Space for both */
		if (PageGetFreeSpace(page) >= combinedSize)
		{
			nbuf = buf;
			npage = page;
			break;
		}

		/* Space for element but not neighbors and last page */
		if (PageGetFreeSpace(page) >= etupSize && !BlockNumberIsValid(HnswPageGetOpaque(page)->nextblkno))
		{
			HnswInsertAppendPage(index, &nbuf, &npage, state, page);
			break;
		}

		/* Space from deleted item */
		if (HnswFreeOffset(index, buf, page, e, ntupSize, &nbuf, &npage, &freeOffno, &freeNeighborOffno, &firstFreePage))
		{
			if (nbuf != buf)
				npage = GenericXLogRegisterBuffer(state, nbuf, 0);

			break;
		}

		insertPage = HnswPageGetOpaque(page)->nextblkno;

		if (BlockNumberIsValid(insertPage))
		{
			/* Move to next page */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
		}
		else
		{
			Buffer		newbuf;
			Page		newpage;

			HnswInsertAppendPage(index, &newbuf, &newpage, state, page);

			/* Commit */
			MarkBufferDirty(newbuf);
			MarkBufferDirty(buf);
			GenericXLogFinish(state);

			/* Unlock previous buffer */
			UnlockReleaseBuffer(buf);

			/* Prepare new buffer */
			state = GenericXLogStart(index);
			buf = newbuf;
			page = GenericXLogRegisterBuffer(state, buf, 0);

			/* Create new page for neighbors if needed */
			if (PageGetFreeSpace(page) < combinedSize)
				HnswInsertAppendPage(index, &nbuf, &npage, state, page);
			else
			{
				nbuf = buf;
				npage = page;
			}

			break;
		}
	}

	e->blkno = BufferGetBlockNumber(buf);
	e->neighborPage = BufferGetBlockNumber(nbuf);

	insertPage = e->neighborPage;

	if (OffsetNumberIsValid(freeOffno))
	{
		e->offno = freeOffno;
		e->neighborOffno = freeNeighborOffno;
	}
	else
	{
		e->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
		if (nbuf == buf)
			e->neighborOffno = OffsetNumberNext(e->offno);
		else
			e->neighborOffno = FirstOffsetNumber;
	}

	ItemPointerSet(&etup->neighbortid, e->neighborPage, e->neighborOffno);

	/* Add element and neighbors */
	if (OffsetNumberIsValid(freeOffno))
	{
		if (!PageIndexTupleOverwrite(page, e->offno, (Item) etup, etupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (!PageIndexTupleOverwrite(npage, e->neighborOffno, (Item) ntup, ntupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}
	else
	{
		if (PageAddItem(page, (Item) etup, etupSize, InvalidOffsetNumber, false, false) != e->offno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (PageAddItem(npage, (Item) ntup, ntupSize, InvalidOffsetNumber, false, false) != e->neighborOffno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}

	/* Commit */
	MarkBufferDirty(buf);
	if (nbuf != buf)
		MarkBufferDirty(nbuf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
	if (nbuf != buf)
		UnlockReleaseBuffer(nbuf);

	/* Update the insert page */
	if (insertPage != originalInsertPage && (!OffsetNumberIsValid(freeOffno) || firstFreePage == insertPage))
		UpdateMetaPage(index, false, NULL, insertPage, MAIN_FORKNUM);
}

/*
 * Calculate index for update
 */
static int
HnswGetIndex(HnswUpdate * update, int m)
{
	return (update->hc.element->level - update->level) * m + update->index;
}

/*
 * Update neighbors
 */
static void
UpdateNeighborPages(Relation index, HnswElement e, int m, List *updates)
{
	ListCell   *lc;

	/* Could update multiple at once for same element */
	/* but should only happen a low percent of time, so keep simple for now */
	foreach(lc, updates)
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		HnswUpdate *update = lfirst(lc);
		ItemId		itemid;
		Size		ntupSize;
		int			idx;
		OffsetNumber offno = update->hc.element->neighborOffno;

		/* Register page */
		buf = ReadBuffer(index, update->hc.element->neighborPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		itemid = PageGetItemId(page, offno);
		ntupSize = ItemIdGetLength(itemid);

		idx = HnswGetIndex(update, m);

		/* Make robust against issues */
		if (idx < (int) HNSW_NEIGHBOR_COUNT(itemid))
		{
			HnswNeighborTuple ntup = (HnswNeighborTuple) PageGetItem(page, itemid);

			HnswNeighborTupleItem *neighbor = &ntup->neighbors[idx];

			/* Set item data */
			ItemPointerSet(&neighbor->indextid, e->blkno, e->offno);
			neighbor->distance = update->hc.distance;

			/* Update connections */
			if (!PageIndexTupleOverwrite(page, offno, (Item) ntup, ntupSize))
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

			/* Commit */
			MarkBufferDirty(buf);
			GenericXLogFinish(state);
		}
		else
			GenericXLogAbort(state);

		UnlockReleaseBuffer(buf);
	}
}

/*
 * Add a heap tid to an existing element
 */
static bool
HnswAddDuplicate(Relation index, HnswElement element, HnswElement dup)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		etupSize = HNSW_ELEMENT_TUPLE_SIZE(dup->vec->dim);
	HnswElementTuple etup;
	int			i;

	/* Read page */
	buf = ReadBuffer(index, dup->blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	/* Find space */
	etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, dup->offno));
	for (i = 0; i < HNSW_HEAPTIDS; i++)
	{
		if (!ItemPointerIsValid(&etup->heaptids[i]))
			break;
	}

	/* Either being deleted or we lost our chance to another backend */
	if (i == 0 || i == HNSW_HEAPTIDS)
	{
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Add heap tid */
	etup->heaptids[i] = *((ItemPointer) linitial(element->heaptids));

	/* Update index tuple */
	if (!PageIndexTupleOverwrite(page, dup->offno, (Item) etup, etupSize))
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	/* Commit */
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	return true;
}

/*
 * Write changes to disk
 */
static void
WriteElement(Relation index, HnswElement element, int m, List *updates, HnswElement dup, HnswElement entryPoint)
{
	/* Try to add to existing page */
	if (dup != NULL)
	{
		if (HnswAddDuplicate(index, element, dup))
			return;
	}

	/* If fails, take this path */
	WriteNewElementPages(index, element, m);
	UpdateNeighborPages(index, element, m, updates);

	/* Update metapage if needed */
	if (entryPoint == NULL || element->level > entryPoint->level)
		UpdateMetaPage(index, true, element, InvalidBlockNumber, MAIN_FORKNUM);
}

/*
 * Insert a tuple into the index
 */
bool
HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
	Datum		value;
	FmgrInfo   *normprocinfo;
	HnswElement entryPoint;
	HnswElement element;
	int			m = HnswGetM(index);
	int			efConstruction = HnswGetEfConstruction(index);
	double		ml = HnswGetMl(m);
	FmgrInfo   *procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	Oid			collation = index->rd_indcollation[0];
	List	   *updates = NIL;
	HnswElement dup;

	/* Detoast once for all calls */
	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize if needed */
	normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	if (normprocinfo != NULL)
	{
		if (!HnswNormValue(normprocinfo, collation, &value, NULL))
			return false;
	}

	/* Create an element */
	element = HnswInitElement(heap_tid, m, ml, HnswGetMaxLevel(m));
	element->vec = DatumGetVector(value);

	/* Get entry point */
	entryPoint = GetEntryPoint(index);

	/* Insert element in graph */
	dup = HnswInsertElement(element, entryPoint, index, procinfo, collation, m, efConstruction, &updates, false);

	/* Write to disk */
	WriteElement(index, element, m, updates, dup, entryPoint);

	return true;
}

/*
 * Insert a tuple into the index
 */
bool
hnswinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
		   Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
		   ,bool indexUnchanged
#endif
		   ,IndexInfo *indexInfo
)
{
	MemoryContext oldCtx;
	MemoryContext insertCtx;

	/* Skip nulls */
	if (isnull[0])
		return false;

	/* Create memory context */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Hnsw insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Insert tuple */
	HnswInsertTuple(index, values, isnull, heap_tid, heap);

	/* Delete memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
