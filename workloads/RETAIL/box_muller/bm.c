/* boxmuller.c           Implements the Polar form of the Box-Muller
                         Transformation

                      (c) Copyright 1994, Everett F. Carter Jr.
                          Permission is granted by the author to use
			  this software for any application provided this
			  copyright notice is preserved.

  2010-04-16: mdunlap -  made this Postgres & GP freindly utilizing the appropriate 
                         PG call macros.
   		      -  could do much better with the seed, for simplicity just hardoding for now. 
  2010-12-14: mdunlap -  Now seeded by time.
                      -  added helper functions to support various APIS
  2010-12-22: mdunlap -  Added half versions per Neglay
*/

#include "postgres.h"
#include "fmgr.h"
#include <math.h>
#include <time.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static int is_seeded = 0;

extern float ranf();         /* ranf() is uniform in 0..1 */
double rand_box_muller( double, double );
int64 C_rand( int64, int64 );
int64 rand_power( int64, int64, int32 );

Datum box_muller( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( box_muller );

Datum power_rand( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( power_rand );

Datum crand( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( crand );

Datum rand_flag( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( rand_flag );

Datum box_muller_mm( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( box_muller_mm );

Datum box_muller_double( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( box_muller_double );

Datum box_muller_half( PG_FUNCTION_ARGS );
PG_FUNCTION_INFO_V1( box_muller_half );

/* The default API box_muller random number generator, called from PG with a mean and standard deviation */
Datum box_muller( PG_FUNCTION_ARGS )
{
	double mean, std, res;

        mean=PG_GETARG_FLOAT8(0);
        std=PG_GETARG_FLOAT8(1);

	res = rand_box_muller( mean, std );
	PG_RETURN_FLOAT8( res );
}


/* The default API power random number generator, called from PG with a min, max and dpower */
Datum power_rand( PG_FUNCTION_ARGS )
{
        int64 min, max, d;
        int64 res;

        min=PG_GETARG_INT64(0);
        max=PG_GETARG_INT64(1);
        d=PG_GETARG_INT32(2);

        res = rand_power( min, max, d );
        PG_RETURN_INT64( res );
}

/* The default API power random number generator, called from PG with a min, max and dpower */
Datum crand( PG_FUNCTION_ARGS )
{
        int64 min, max;
        int64 res;

        min=PG_GETARG_INT64(0);
        max=PG_GETARG_INT64(1);

        res = C_rand( min, max );
        PG_RETURN_INT64( res );
}


/* The default API box_muller rabdom number generator, called from PG with a mean and standard deviation */
Datum box_muller_mm( PG_FUNCTION_ARGS )
{
        int64 min, max, res; 
        int16 std_off;
        double mean, std; 
        bool honor_bounds;
        short ctr=0; 

        min=PG_GETARG_INT64(0);
        max=PG_GETARG_INT64(1);
        res=min+max;

        if ( PG_NARGS() >= 3 ) { 
           std_off=PG_GETARG_INT16(2);
        } else {
           std_off=4;
        }

        if ( PG_NARGS() == 4 ) { 
           honor_bounds=PG_GETARG_BOOL(3);
        } else {
           honor_bounds=1;
        }

        mean = min + ((max - min)/2.0);
        std = ((max - min)/2.0)/std_off;

        do { 
           ctr++;
           res = llround( rand_box_muller( mean, std )) ;
           } while ( honor_bounds && ctr < 10 && ( res < min || res > max ) );

        if ( honor_bounds && ctr == 10 && ( res < min || res > max ) )
           res = llround( mean ) ;

        PG_RETURN_INT64( res );
}

Datum box_muller_double( PG_FUNCTION_ARGS )
{
        int64 min, max, min1, max1, min2, max2, res;
        int16 std_off;
        double mean, std;
        bool honor_bounds;
        short ctr=0;
        static short itr=0;

        min1=PG_GETARG_INT64(0);
        max1=PG_GETARG_INT64(1);
        min2=PG_GETARG_INT64(2);
        max2=PG_GETARG_INT64(3);

        if ( PG_NARGS() >= 5 ) {
           std_off=PG_GETARG_INT16(4);
        } else {
           std_off=4;
        }

        if ( PG_NARGS() == 6 ) {
           honor_bounds=PG_GETARG_BOOL(5);
        } else {
           honor_bounds=1;
        }

        if ( itr ) {
           min = min2;
           max = max2;
        } else {
           min = min1;
           max = max1;
        }

        itr=(itr+1)%2;
    
        mean = min + ((max - min)/2.0);
        std = ((max - min)/2.0)/std_off;

        do {
           ctr++;
           res = llround( rand_box_muller( mean, std )) ;
           } while ( honor_bounds && ctr < 10 && ( res < min || res > max ) );

        if ( honor_bounds && ctr == 10 && ( res < min || res > max ) )
           res = llround( mean ) ;

        PG_RETURN_INT64( res );
}

/* Generate a random number based on the right half of a normal curve */
Datum box_muller_half( PG_FUNCTION_ARGS )
{
        int64 min, max, res;
        int16 std_off;
        double mean, std, tres;
        bool honor_bounds;
        short ctr=0;

        min=PG_GETARG_INT64(0);
        max=PG_GETARG_INT64(1);

        if ( PG_NARGS() >= 3 ) {
           std_off=PG_GETARG_INT16(2);
        } else {
           std_off=3;
        }

        if ( PG_NARGS() == 4 ) {
           honor_bounds=PG_GETARG_BOOL(3);
        } else {
           honor_bounds=1;
        }

        mean = min;
        std = ((max - min)/2.0)/std_off;

        do {
           ctr++;
           tres = rand_box_muller( mean, std );
           if ( tres < min )
              tres = abs(tres)+min;
           res = llround( tres ) ;
           } while ( honor_bounds && ctr < 10 && ( res > max ) );

        if ( honor_bounds && ctr == 10 && ( res > max ) )
           res = llround( mean ) ;

        PG_RETURN_INT64( res );
}



double rand_box_muller( mean, std )
double mean, std;
{
        double x1, x2, w, y1;
        static double y2;
        static int use_last = 0;

        if ( ! is_seeded) {     /* First time through, seed the random number */
           srand( (unsigned int)time( NULL ) );
           is_seeded = 1;
        }

        if (use_last) {         /* use value from previous call */
           y1 = y2;
           use_last = 0;
        } else {
                do {
                       x1 = 2.0 * ( (double)rand() / ( (double)(RAND_MAX)+1.0 ) ) - 1.0;
                       x2 = 2.0 * ( (double)rand() / ( (double)(RAND_MAX)+1.0 ) ) - 1.0;
                       w = x1 * x1 + x2 * x2;
                } while ( w >= 1.0 );

                w = sqrt( (-2.0 * log( w ) ) / w );
                y1 = x1 * w;
                y2 = x2 * w;
                use_last = 1;
        }

        return( mean + y1 * std );
}

int64 rand_power( min, max, n )
int64 min, max;
int32 n;
{
   int64 res;

  if ( ! is_seeded) {     /* First time through, seed the random number */
      srand( (unsigned int)time( NULL ) );
      is_seeded = 1;
  }

   max+=1;
   res = llround( 
              pow(((pow(max,(n+1)) - pow(min,(n+1)))*( (double)rand() / ( (double)(RAND_MAX)+1.0 ) ) + pow(min,(n+1))),(1.0/(n+1))) 
                + 0.5 );
  
   return( max - res + min );
   
}

int64 C_rand( min, max )
int64 min, max;
{
   int64 res;

  if ( ! is_seeded) {     /* First time through, seed the random number */
      srand( (unsigned int)time( NULL ) );
      is_seeded = 1;
  }

   res = min + llround( (max-min+1)*( (double)rand() / ( (double)(RAND_MAX)+1.0 ) ) + 0.5 ) - 1;
  
   return( res );

   
}

Datum rand_flag( PG_FUNCTION_ARGS )
{
    double pct_likely;

    if ( ! is_seeded) {     /* First time through, seed the random number */
      srand( (unsigned int)time( NULL ) );
      is_seeded = 1;
    }

    pct_likely=PG_GETARG_FLOAT8(0);
    if ( pct_likely < ( (double)rand() / (double)(RAND_MAX) ) ) 
       PG_RETURN_BOOL( false );

    PG_RETURN_BOOL( true );

}

