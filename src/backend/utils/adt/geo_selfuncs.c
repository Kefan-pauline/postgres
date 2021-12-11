/*-------------------------------------------------------------------------
 *
 * geo_selfuncs.c
 *	  Selectivity routines registered in the operator catalog in the
 *	  "oprrest" and "oprjoin" attributes.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/geo_selfuncs.c
 *
 *	XXX These are totally bogus.  Perhaps someone will make them do
 *	something reasonable, someday.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/geo_decls.h"

#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/rangetypes.h"


/*
 *	Selectivity functions for geometric operators.  These are bogus -- unless
 *	we know the actual key distribution in the index, we can't make a good
 *	prediction of the selectivity of these operators.
 *
 *	Note: the values used here may look unreasonably small.  Perhaps they
 *	are.  For now, we want to make sure that the optimizer will make use
 *	of a geometric index if one is available, so the selectivity had better
 *	be fairly small.
 *
 *	In general, GiST needs to search multiple subtrees in order to guarantee
 *	that all occurrences of the same key have been found.  Because of this,
 *	the estimated cost for scanning the index ought to be higher than the
 *	output selectivity would indicate.  gistcostestimate(), over in selfuncs.c,
 *	ought to be adjusted accordingly --- but until we can generate somewhat
 *	realistic numbers here, it hardly matters...
 */

/*
 * Range Overlaps Join Selectivity.
 */
Datum
rangeoverlapsjoinsel(PG_FUNCTION_ARGS)
{

    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid         operator = PG_GETARG_OID(1);
    List       *args = (List *) PG_GETARG_POINTER(2);     /* left and right columns for joins*/
    JoinType    jointype = (JoinType) PG_GETARG_INT16(3);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid         collation = PG_GET_COLLATION();

    double      selec = 0.005;

    VariableStatData vardata1;
    VariableStatData vardata2;
    Oid         opfuncoid;
    AttStatsSlot sslot1; // bound hist for column 1
    AttStatsSlot sslot2; // overlap hist for column 1
    AttStatsSlot sslot3; // length hist for column 1
    AttStatsSlot sslot4; // overlap hist for column 2
    AttStatsSlot sslot5; // length hist for column 2
    int         nhist;
    int         nhist2;
    int         len;
    int         len2;
    RangeBound *hist_lower1;
    RangeBound *hist_upper1;
    float*        hist_overlap;  
    float*        hist_overlap2;  
    int         i;
    Form_pg_statistic stats1 = NULL;
    TypeCacheEntry *typcache = NULL;
    bool        join_is_reversed;
    bool        empty;
    int         loop_max;
    int         result = 0;
    float       mu1 = 0;
    float       mu2 = 0;
    
    


    get_join_variables(root, args, sjinfo,                        /* store data of left and right columns */
                       &vardata1, &vardata2, &join_is_reversed);

    typcache = range_get_typcache(fcinfo, vardata1.vartype);
    opfuncoid = get_opcode(operator);
    
    memset(&sslot1, 0, sizeof(sslot1));
    memset(&sslot2, 0, sizeof(sslot2));
    memset(&sslot3, 0, sizeof(sslot3));
    memset(&sslot4, 0, sizeof(sslot4));
    memset(&sslot5, 0, sizeof(sslot5));
    
    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata1, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);

    if (HeapTupleIsValid(vardata1.statsTuple))
    {
        stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        /* Try to get fraction of empty ranges */
        if (!get_attstatsslot(&sslot1, vardata1.statsTuple,        /* store in slot1 (5 in total) the histogram, here the bound hist */
                             STATISTIC_KIND_BOUNDS_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot2, vardata1.statsTuple,        /* store in slot2 (5 in total) the histogram, here the overlap hist */
                             STATISTIC_KIND_OVERLAP_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot3, vardata1.statsTuple,        /* store in slot2 (5 in total) the histogram, here the len hist */
                             STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot4, vardata2.statsTuple,        /* store in slot2 (5 in total) the histogram, here the overlap hist */
                             STATISTIC_KIND_OVERLAP_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot5, vardata2.statsTuple,        /* store in slot2 (5 in total) the histogram, here the len hist */
                             STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
    }

    nhist = sslot1.nvalues;    /* split the bounds into lower and upper : bound hist contains ranges, we deserialize */
    
    hist_lower1 = (RangeBound *) palloc(sizeof(RangeBound) * nhist);
    hist_upper1 = (RangeBound *) palloc(sizeof(RangeBound) * nhist);
    for (i = 0; i < nhist; i++)
    {
        range_deserialize(typcache, DatumGetRangeTypeP(sslot1.values[i]),
                          &hist_lower1[i], &hist_upper1[i], &empty);
        /* The histogram should not contain any empty ranges */
        if (empty)
            elog(ERROR, "bounds histogram contains an empty range");
    }
    
    /*
    printf("hist_lower = [");
    for (i = 0; i < nhist; i++)
    {
        printf("%d", DatumGetInt16(hist_lower1[i].val));
        if (i < nhist - 1)
            printf(", ");
    }
    printf("]\n");
    printf("hist_upper = [");
    for (i = 0; i < nhist; i++)
    {
        printf("%d", DatumGetInt16(hist_upper1[i].val));
        if (i < nhist - 1)
            printf(", ");
    }
    printf("]\n");*/
    




    len = sslot2.nvalues;
    hist_overlap = (float*) palloc(sizeof(float)*len);

    for (i = 0; i < len; i++)
    {
        hist_overlap[i] = DatumGetFloat8(sslot2.values[i]);
                        
    }
    

    printf("len : %d \n",len);
    
    printf("hist_overlap = [");
    for (i = 0; i < len; i++)
    {
        printf("%f", hist_overlap[i]);
        if (i < len - 1)
            printf(", ");
    }
    printf("]\n");
    
    
    
    

    
    for (i = 0; i < nhist; i++)
    {
        mu1 += DatumGetFloat8(sslot3.values[i]);
    }
    mu1 = mu1 / nhist;
    
    
    
    nhist2 = sslot5.nvalues; 
    for (i = 0; i < nhist2; i++)
    {
        mu2 += DatumGetFloat8(sslot5.values[i]);
    }
    mu2 = mu2 / nhist2;

    
    
    
    len2 = sslot4.nvalues;
    hist_overlap2 = (float*) palloc(sizeof(float)*len2);

    for (i = 0; i < len2; i++)
    {
        hist_overlap2[i] = DatumGetFloat8(sslot4.values[i]);
                        
    }
    

    
    
    if (len<len2){
    	loop_max = len;
    }else{
        loop_max = len2;
    }

    // join estimation
    for (i=0;i<loop_max;i++){
    	result += hist_overlap[i]  * hist_overlap2[i] ;
    }
    
    printf("%d\n",result);
    
    // normalize result (to change) 

    //result = result / ((mu1/2)*(mu2/2));
    selec = result / (float)(nhist * nhist2);
 
    printf("%f\n",selec);

    pfree(hist_lower1);
    pfree(hist_upper1);
    pfree(hist_overlap);
    pfree(hist_overlap2);

    free_attstatsslot(&sslot1);
    free_attstatsslot(&sslot2);
    free_attstatsslot(&sslot3);
    free_attstatsslot(&sslot4);
    free_attstatsslot(&sslot5);

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}

/*
 * Selectivity for operators that depend on area, such as "overlap".
 */

Datum
areasel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

Datum
areajoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

/*
 *	positionsel
 *
 * How likely is a box to be strictly left of (right of, above, below)
 * a given box?
 */

Datum
positionsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

Datum
positionjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

/*
 *	contsel -- How likely is a box to contain (be contained by) a given box?
 *
 * This is a tighter constraint than "overlap", so produce a smaller
 * estimate than areasel does.
 */

Datum
contsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}

Datum
contjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}
