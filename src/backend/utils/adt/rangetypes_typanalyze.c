/*-------------------------------------------------------------------------
 *
 * rangetypes_typanalyze.c
 *	  Functions for gathering statistics from range columns
 *
 * For a range type column, histograms of lower and upper bounds, and
 * the fraction of NULL and empty ranges are collected.
 *
 * Both histograms have the same length, and they are combined into a
 * single array of ranges. This has the same shape as the histogram that
 * std_typanalyze would collect, but the values are different. Each range
 * in the array is a valid range, even though the lower and upper bounds
 * come from different tuples. In theory, the standard scalar selectivity
 * functions could be used with the combined histogram.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/rangetypes_typanalyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_operator.h"
#include "commands/vacuum.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rangetypes.h"
#include "utils/multirangetypes.h"


typedef struct histogram {

	int steps;
	Datum low; // min value of ranges
	Datum max; // max value of ranges
	int *values;

}histogram, *histogram_t;

typedef struct dict_entry_s {

	RangeType *key;
	int value;

} dict_entry;

typedef struct dict_mcv {

	int len;
	int limit_entry;
	dict_entry *entry;
	
} dict_mcv, *dict_t;

static dict_t dict;
static RangeType *key;


static int	float8_qsort_cmp(const void *a1, const void *a2);
static int	range_bound_qsort_cmp(const void *a1, const void *a2, void *arg);
static void compute_range_stats(VacAttrStats *stats,
								AnalyzeAttrFetchFunc fetchfunc, int samplerows,
								double totalrows);
								
static histogram_t histogram_new(int step, Datum low, Datum max,int length); 

static void dict_add(dict_t dict, RangeType *key);
static dict_t dict_new(void);
static void dict_free(dict_t dict);
static void display_dict(dict_t dict, RangeType *key);

/*
 * range_typanalyze -- typanalyze function for range columns
 */
Datum
range_typanalyze(PG_FUNCTION_ARGS)
{
	VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);
	TypeCacheEntry *typcache;
	Form_pg_attribute attr = stats->attr;

	/* Get information about range type; note column might be a domain */
	typcache = range_get_typcache(fcinfo, getBaseType(stats->attrtypid));

	if (attr->attstattarget < 0)
		attr->attstattarget = default_statistics_target;

	stats->compute_stats = compute_range_stats;
	stats->extra_data = typcache;
	/* same as in std_typanalyze */
	stats->minrows = 300 * attr->attstattarget;
	PG_RETURN_BOOL(true);
}

/*
 * multirange_typanalyze -- typanalyze function for multirange columns
 *
 * We do the same analysis as for ranges, but on the smallest range that
 * completely includes the multirange.
 */
Datum
multirange_typanalyze(PG_FUNCTION_ARGS)
{
	VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);
	TypeCacheEntry *typcache;
	Form_pg_attribute attr = stats->attr;

	/* Get information about multirange type; note column might be a domain */
	typcache = multirange_get_typcache(fcinfo, getBaseType(stats->attrtypid));

	if (attr->attstattarget < 0)
		attr->attstattarget = default_statistics_target;

	stats->compute_stats = compute_range_stats;
	stats->extra_data = typcache;
	/* same as in std_typanalyze */
	stats->minrows = 300 * attr->attstattarget;

	PG_RETURN_BOOL(true);
}

/*
 * Comparison function for sorting float8s, used for range lengths.
 */
static int
float8_qsort_cmp(const void *a1, const void *a2)
{
	const float8 *f1 = (const float8 *) a1;
	const float8 *f2 = (const float8 *) a2;

	if (*f1 < *f2)
		return -1;
	else if (*f1 == *f2)
		return 0;
	else
		return 1;
}

/*
 * Comparison function for sorting RangeBounds.
 */
static int
range_bound_qsort_cmp(const void *a1, const void *a2, void *arg)
{
	RangeBound *b1 = (RangeBound *) a1;
	RangeBound *b2 = (RangeBound *) a2;
	TypeCacheEntry *typcache = (TypeCacheEntry *) arg;

	return range_cmp_bounds(typcache, b1, b2);
}



/*
 * Overlaping histogram functions.
 */
 
static void display_values(histogram_t hist,int length){
	int i;
	for(i=0;i<length;i++){
		printf("%d\n",hist->values[i]);
		fflush(stdout);
	}
}

static histogram_t histogram_new(int step, Datum low, Datum max, int length) {

	histogram hist = {step, low, max, calloc(length, sizeof(int)) };
	histogram_t res = malloc(sizeof(histogram));
	*res = hist;
	return res;
	
}

static int convert_bound_to_index(Datum bound, int step){
	int* pointer1;
	pointer1 = (int*) bound;
	//printf("%d\n",(*pointer1/ step));
	return (int) (*pointer1/ step);
}


/*
 * MCV functions.
 */
 
int dict_find_index(PG_FUNCTION_ARGS) {
 
 	bool res;
 	TypeCacheEntry *typcache;
 	
 	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(key));
 	
 	for(int i = 0; i < dict->len; i++) {
 	
 		res = range_eq_internal(typcache,dict->entry[i].key, key);
 		
 		if(res) { return i;}
		
	}

 }
 
static void dict_add(dict_t dict, RangeType *key) {

	
	int index = dict_find_index("nothing");
	
	if(dict->len == dict->limit_entry) {
		dict->limit_entry *= 2;
		dict->entry = realloc(dict->entry, dict->limit_entry * sizeof(dict_entry));
	}
	
	if(index != -1) {
		dict->entry[index].value += 1;
		return;
	}
	
	dict->entry[dict->len].key = strdup(key);
	dict->entry[dict->len].value = 1;
	dict->len++;
	
}

static dict_t dict_new(void) {
	
	dict_t res;
	
	dict_mcv dict = {0, 20, malloc(20 * sizeof(dict_entry)) };
	res = malloc(sizeof(dict_mcv));
	*res = dict;
	return res;
	
}


static void dict_free(dict_t dict) {


	for(int i = 0; i < dict->len; i++) {
		free(dict->entry[i].key);
	} 
	
	free(dict->entry);
	free(dict);

}


static void display_dict(dict_t dict, RangeType *key) {
	
	int index;

	index = dict_find_index("nothing");
	
	if(index != -1) {
		//printf("Key %s : %d\n", key, dict->entry[index].value);
		return;
	}
	
	else {
		printf("Erreur Key not found !\n");
	}
}

/*
 * compute_range_stats() -- compute statistics for a range column
 */
static void
compute_range_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
					int samplerows, double totalrows)
{
	TypeCacheEntry *typcache = (TypeCacheEntry *) stats->extra_data;
	TypeCacheEntry *mltrng_typcache = NULL;
	bool		has_subdiff;
	int			null_cnt = 0;
	int			non_null_cnt = 0;
	int			non_empty_cnt = 0;
	int			empty_cnt = 0;
	int			range_no;
	int			slot_idx;
	int			num_bins = stats->attr->attstattarget;
	int			num_hist;
	float8	   *lengths;
	RangeBound *lowers,
			   *uppers;
	double		total_width = 0;
	histogram_t overlap_hist;

	if (typcache->typtype == TYPTYPE_MULTIRANGE)
	{
		mltrng_typcache = typcache;
		typcache = typcache->rngtype;
	}
	else
		Assert(typcache->typtype == TYPTYPE_RANGE);
	has_subdiff = OidIsValid(typcache->rng_subdiff_finfo.fn_oid);

	/* Allocate memory to hold range bounds and lengths of the sample ranges. */
	lowers = (RangeBound *) palloc(sizeof(RangeBound) * samplerows);
	uppers = (RangeBound *) palloc(sizeof(RangeBound) * samplerows);
	lengths = (float8 *) palloc(sizeof(float8) * samplerows);
	

	/* Loop over the sample ranges. */
	for (range_no = 0; range_no < samplerows; range_no++)
	{
		Datum		value;
		bool		isnull,
					empty;
		MultirangeType *multirange;
		RangeType  *range;
		RangeBound	lower,
					upper;
		float8		length;

		vacuum_delay_point();

		value = fetchfunc(stats, range_no, &isnull);
		if (isnull)
		{
			/* range is null, just count that */
			null_cnt++;
			continue;
		}

		/*
		 * XXX: should we ignore wide values, like std_typanalyze does, to
		 * avoid bloating the statistics table?
		 */
		total_width += VARSIZE_ANY(DatumGetPointer(value));

		/* Get range and deserialize it for further analysis. */
		if (mltrng_typcache != NULL)
		{
			/* Treat multiranges like a big range without gaps. */
			multirange = DatumGetMultirangeTypeP(value);
			if (!MultirangeIsEmpty(multirange))
			{
				RangeBound	tmp;

				multirange_get_bounds(typcache, multirange, 0,
									  &lower, &tmp);
				multirange_get_bounds(typcache, multirange,
									  multirange->rangeCount - 1,
									  &tmp, &upper);
				empty = false;
			}
			else
			{
				empty = true;
			}
		}
		else
		{
			range = DatumGetRangeTypeP(value);
			range_deserialize(typcache, range, &lower, &upper, &empty);
		}

		if (!empty)
		{
			/* Remember bounds and length for further usage in histograms */
			lowers[non_empty_cnt] = lower;
			uppers[non_empty_cnt] = upper;

			if (lower.infinite || upper.infinite)
			{
				/* Length of any kind of an infinite range is infinite */
				length = get_float8_infinity();
			}
			else if (has_subdiff)
			{
				/*
				 * For an ordinary range, use subdiff function between upper
				 * and lower bound values.
				 */
				length = DatumGetFloat8(FunctionCall2Coll(&typcache->rng_subdiff_finfo,
														  typcache->rng_collation,
														  upper.val, lower.val));
			}
			else
			{
				/* Use default value of 1.0 if no subdiff is available. */
				length = 1.0;
			}
			lengths[non_empty_cnt] = length;

			non_empty_cnt++;
		}
		else
			empty_cnt++;

		non_null_cnt++;
	}
	

	slot_idx = 0;

	/* We can only compute real stats if we found some non-null values. */
	if (non_null_cnt > 0)
	{
		Datum	   *bound_hist_values;
		Datum	   *length_hist_values;
		int			pos,
					posfrac,
					delta,
					deltafrac,
					i;
		MemoryContext old_cxt;
		float4	   *emptyfrac;

		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;
		stats->stawidth = total_width / (double) non_null_cnt;

		/* Estimate that non-null values are unique */
		stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);

		/* Must copy the target values into anl_context */
		old_cxt = MemoryContextSwitchTo(stats->anl_context);


		/*
		 * Generate a bounds histogram slot entry if there are at least two
		 * values.
		 */
		if (non_empty_cnt >= 2)
		{
			Datum hist_low;
			Datum hist_up;
			int step;
			int index_low;
			int index_up;
			int j;
			int len;
			
			/* Sort bound values */
			qsort_arg(lowers, non_empty_cnt, sizeof(RangeBound),
					  range_bound_qsort_cmp, typcache);
			qsort_arg(uppers, non_empty_cnt, sizeof(RangeBound),
					  range_bound_qsort_cmp, typcache);

			hist_low = *(int*)PointerGetDatum(&lowers[0]);
			hist_up = *(int*)PointerGetDatum(&uppers[samplerows-1]);
			step = 2;
			len = (hist_up - hist_low)/step;
			
			overlap_hist = histogram_new(step,hist_low,hist_up,len);
			
			num_hist = non_empty_cnt;
			if (num_hist > num_bins)
				num_hist = num_bins + 1;

			bound_hist_values = (Datum *) palloc(num_hist * sizeof(Datum));

			/*
			 * The object of this loop is to construct ranges from first and
			 * last entries in lowers[] and uppers[] along with evenly-spaced
			 * values in between. So the i'th value is a range of lowers[(i *
			 * (nvals - 1)) / (num_hist - 1)] and uppers[(i * (nvals - 1)) /
			 * (num_hist - 1)]. But computing that subscript directly risks
			 * integer overflow when the stats target is more than a couple
			 * thousand.  Instead we add (nvals - 1) / (num_hist - 1) to pos
			 * at each step, tracking the integral and fractional parts of the
			 * sum separately.
			 */
			delta = (non_empty_cnt - 1) / (num_hist - 1);
			deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
			pos = posfrac = 0;

			for (i = 0; i < num_hist; i++)
			{
				bound_hist_values[i] = PointerGetDatum(range_serialize(typcache,
																	   &lowers[pos],
																	   &uppers[pos],
																	   false));
				index_low = convert_bound_to_index(PointerGetDatum(&lowers[pos]), step);												   
				index_up = convert_bound_to_index(PointerGetDatum(&uppers[pos]), step);
				for (j = index_low; j <= index_up; j++){
					overlap_hist->values[j]++;
				}	
						   
				pos += delta;
				posfrac += deltafrac;
				if (posfrac >= (num_hist - 1))
				{
					/* fractional part exceeds 1, carry to integer part */
					pos++;
					posfrac -= (num_hist - 1);
				}
			}
			
			

			stats->stakind[slot_idx] = STATISTIC_KIND_BOUNDS_HISTOGRAM;   /* need a new cst */
			stats->stavalues[slot_idx] = bound_hist_values;     /* save value as range */
			stats->numvalues[slot_idx] = num_hist;

			/* Store ranges even if we're analyzing a multirange column */
			stats->statypid[slot_idx] = typcache->type_id;
			stats->statyplen[slot_idx] = typcache->typlen;
			stats->statypbyval[slot_idx] = typcache->typbyval;
			stats->statypalign[slot_idx] = typcache->typalign;

			slot_idx++;
			
			/* stock overlap histgram */
			stats->stakind[slot_idx] = STATISTIC_KIND_OVERLAP_HISTOGRAM;   /* need a new cst */
			stats->stavalues[slot_idx] = overlap_hist->values;     
			stats->numvalues[slot_idx] = len;

			/* Store ranges even if we're analyzing a multirange column */
			stats->statypid[slot_idx] = FLOAT8OID;              /* 4 lines : length hist is float values */
			stats->statyplen[slot_idx] = sizeof(float8);
			stats->statypbyval[slot_idx] = FLOAT8PASSBYVAL;
			stats->statypalign[slot_idx] = 'd';

			
			slot_idx++;
			
		}
		


		/*
		 * Generate a length histogram slot entry if there are at least two
		 * values.
		 */
		if (non_empty_cnt >= 2)
		{
			/*
			 * Ascending sort of range lengths for further filling of
			 * histogram
			 */
			qsort(lengths, non_empty_cnt, sizeof(float8), float8_qsort_cmp);

			num_hist = non_empty_cnt;
			if (num_hist > num_bins)
				num_hist = num_bins + 1;

			length_hist_values = (Datum *) palloc(num_hist * sizeof(Datum));

			/*
			 * The object of this loop is to copy the first and last lengths[]
			 * entries along with evenly-spaced values in between. So the i'th
			 * value is lengths[(i * (nvals - 1)) / (num_hist - 1)]. But
			 * computing that subscript directly risks integer overflow when
			 * the stats target is more than a couple thousand.  Instead we
			 * add (nvals - 1) / (num_hist - 1) to pos at each step, tracking
			 * the integral and fractional parts of the sum separately.
			 */
			delta = (non_empty_cnt - 1) / (num_hist - 1);
			deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
			pos = posfrac = 0;

			for (i = 0; i < num_hist; i++)
			{
				length_hist_values[i] = Float8GetDatum(lengths[pos]);
				pos += delta;
				posfrac += deltafrac;
				if (posfrac >= (num_hist - 1))
				{
					/* fractional part exceeds 1, carry to integer part */
					pos++;
					posfrac -= (num_hist - 1);
				}
			}
		}
		else
		{
			/*
			 * Even when we don't create the histogram, store an empty array
			 * to mean "no histogram". We can't just leave stavalues NULL,
			 * because get_attstatsslot() errors if you ask for stavalues, and
			 * it's NULL. We'll still store the empty fraction in stanumbers.
			 */
			length_hist_values = palloc(0);
			num_hist = 0;
		}
		stats->staop[slot_idx] = Float8LessOperator;
		stats->stacoll[slot_idx] = InvalidOid;
		stats->stavalues[slot_idx] = length_hist_values;
		stats->numvalues[slot_idx] = num_hist;
		stats->statypid[slot_idx] = FLOAT8OID;              /* 4 lines : length hist is float values */
		stats->statyplen[slot_idx] = sizeof(float8);
		stats->statypbyval[slot_idx] = FLOAT8PASSBYVAL;
		stats->statypalign[slot_idx] = 'd';

		/* Store the fraction of empty ranges */
		emptyfrac = (float4 *) palloc(sizeof(float4));
		*emptyfrac = ((double) empty_cnt) / ((double) non_null_cnt);
		stats->stanumbers[slot_idx] = emptyfrac;  /* a list of float used for empty ranges */
		stats->numnumbers[slot_idx] = 1;

		stats->stakind[slot_idx] = STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM;
		slot_idx++;

		MemoryContextSwitchTo(old_cxt);
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;
		stats->stawidth = 0;	/* "unknown" */
		stats->stadistinct = 0.0;	/* "unknown" */
	}

	/*
	 * We don't need to bother cleaning up any of our temporary palloc's. The
	 * hashtable should also go away, as it used a child memory context.
	 */
}


