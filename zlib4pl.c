/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        wielemak@science.uva.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2006, University of Amsterdam

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/

#define O_DEBUG 1
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <zlib.h>

install_t install_zlib4pl(void);

static atom_t ATOM_format;		/* format(Format) */
static atom_t ATOM_level;		/* level(Int) */
static atom_t ATOM_close_parent;	/* close_parent(Bool) */
static atom_t ATOM_gzip;
static atom_t ATOM_deflate;
static int debuglevel = 0;

#ifdef O_DEBUG
#define DEBUG(n, g) if ( debuglevel >= n ) g
#else
#define DEBUG(n, g) (void)0
#endif

		 /*******************************
		 *	       TYPES		*
		 *******************************/

#define BUFSIZE SIO_BUFSIZE		/* raw I/O buffer */

typedef enum
{ F_UNKNOWN = 0,
  F_GZIP,				/* gzip output */
  F_DEFLATE				/* zlib data */
} zformat;

typedef struct z_context
{ IOSTREAM	   *stream;		/* Original stream */
  IOSTREAM	   *zstream;		/* Compressed stream (I'm handle of) */
  int		    close_parent;	/* close parent on close */
  int		    initialized;	/* did inflateInit()? */
  zformat	    format;		/* current format */
  z_stream	    zstate;		/* Zlib state */
} z_context;


static z_context*
alloc_zcontext(IOSTREAM *s)
{ z_context *ctx = PL_malloc(sizeof(*ctx));

  memset(ctx, 0, sizeof(*ctx));
  ctx->stream       = s;
  ctx->close_parent = TRUE;

  return ctx;
}


static void
free_zcontext(z_context *ctx)
{ if ( ctx->stream->upstream )
    Sset_filter(ctx->stream, NULL);
  else
    PL_release_stream(ctx->stream);

  PL_free(ctx);
}


		 /*******************************
		 *	       GZ I/O		*
		 *******************************/

static void
sync_stream(z_context *ctx)
{ ctx->stream->bufp   = (char*)ctx->zstate.next_in;
}


static ssize_t				/* inflate */
zread(void *handle, char *buf, size_t size)
{ z_context *ctx = handle;
  int flush = Z_SYNC_FLUSH;
  int rc;

  if ( ctx->zstate.avail_in == 0 )
  { if ( Sfeof(ctx->stream) )
    { flush = Z_FINISH;
    } else
    { ctx->zstate.next_in  = (Bytef*)ctx->stream->bufp;
      ctx->zstate.avail_in = (long)(ctx->stream->limitp - ctx->stream->bufp);
      DEBUG(1, Sdprintf("Set avail_in to %d\n", ctx->zstate.avail_in));
    }
  }

  DEBUG(1, Sdprintf("Processing %d bytes\n", ctx->zstate.avail_in));
  ctx->zstate.next_out  = (Bytef*)buf;
  ctx->zstate.avail_out = (long)size;

  if ( ctx->initialized == FALSE )
  { if ( ctx->format == F_GZIP )
    { rc = inflateInit2(&ctx->zstate, MAX_WBITS+16);
    } else if ( ctx->format == F_DEFLATE )
    { rc = inflateInit(&ctx->zstate);
    } else
    { rc = inflateInit2(&ctx->zstate, MAX_WBITS+32);
    }
    ctx->initialized = TRUE;
    sync_stream(ctx);
  }

  rc = inflate(&ctx->zstate, Z_NO_FLUSH);
  sync_stream(ctx);

  switch( rc )
  { case Z_OK:
    case Z_STREAM_END:
    { long n = (long)(size - ctx->zstate.avail_out);

      if ( rc == Z_STREAM_END )
      { DEBUG(1, Sdprintf("Z_STREAM_END: %d bytes\n", n));
      } else
      { DEBUG(1, Sdprintf("inflate(): Z_OK: %d bytes\n", n));
	if (n == 0 && rc != Z_STREAM_END)
        { /* If we get here then there was not enough data in the in buffer to decode
             a single character, but we are not at the end of the stream, so we must read
	     more from the parent */
          DEBUG(1, Sdprintf("Not enough data to decode.  Retrying\n"));

          return zread(handle, buf, size);
	}
      }

      return n;
    }
    case Z_NEED_DICT:
      DEBUG(1, Sdprintf("Z_NEED_DICT\n"));
      break;
    case Z_DATA_ERROR:
      DEBUG(1, Sdprintf("Z_DATA_ERROR\n"));
      break;
    case Z_STREAM_ERROR:
      DEBUG(1, Sdprintf("Z_STREAM_ERROR\n"));
      break;
    case Z_MEM_ERROR:
      DEBUG(1, Sdprintf("Z_MEM_ERROR\n"));
      break;
    case Z_BUF_ERROR:
      DEBUG(1, Sdprintf("Z_BUF_ERROR\n"));
      break;
    default:
      DEBUG(1, Sdprintf("Inflate error: %d\n", rc));
  }
  if ( ctx->zstate.msg )
    Sdprintf("ERROR: zread(): %s\n", ctx->zstate.msg);
  return -1;
}


static ssize_t				/* deflate */
zwrite4(void *handle, char *buf, size_t size, int flush)
{ z_context *ctx = handle;
  Bytef buffer[SIO_BUFSIZE];
  int rc;
  int loops = 0;

  ctx->zstate.next_in = (Bytef*)buf;
  ctx->zstate.avail_in = (long)size;

  DEBUG(1, Sdprintf("Compressing %d bytes\n", ctx->zstate.avail_in));
  do
  { loops++;
    ctx->zstate.next_out  = buffer;
    ctx->zstate.avail_out = sizeof(buffer);

    switch( (rc = deflate(&ctx->zstate, flush)) )
    { case Z_OK:
      case Z_STREAM_END:
      { size_t n = sizeof(buffer) - ctx->zstate.avail_out;

	DEBUG(1, Sdprintf("Compressed (%s) to %d bytes; left %d\n",
			  rc == Z_OK ? "Z_OK" : "Z_STREAM_END",
			  n, ctx->zstate.avail_in));

	if ( Sfwrite(buffer, 1, n, ctx->stream) != n )
	  return -1;

	break;
      }
      case Z_BUF_ERROR:
	DEBUG(1, Sdprintf("zwrite4(): Z_BUF_ERROR\n"));
        break;
      case Z_STREAM_ERROR:
      default:
	Sdprintf("ERROR: zwrite(): %s\n", ctx->zstate.msg);
	return -1;
    }
  } while ( ctx->zstate.avail_in > 0 ||
	    (flush != Z_NO_FLUSH && rc == Z_OK) );

  if ( flush != Z_NO_FLUSH && Sflush(ctx->stream) < 0 )
    return -1;

  return size;
}


static ssize_t				/* deflate */
zwrite(void *handle, char *buf, size_t size)
{ return zwrite4(handle, buf, size, Z_NO_FLUSH);
}


static int
zcontrol(void *handle, int op, void *data)
{ z_context *ctx = handle;

  switch(op)
  { case SIO_FLUSHOUTPUT:
      DEBUG(1, Sdprintf("Flushing output\n"));
      return (int)zwrite4(handle, NULL, 0, Z_SYNC_FLUSH);
    case SIO_SETENCODING:
      return 0;				/* allow switching encoding */
    default:
      if ( ctx->stream->functions->control )
	return (*ctx->stream->functions->control)(ctx->stream->handle, op, data);
      return -1;
  }
}


static int
zclose(void *handle)
{ z_context *ctx = handle;
  ssize_t rc;

  DEBUG(1, Sdprintf("zclose() ...\n"));

  if ( (ctx->stream->flags & SIO_INPUT) )
  { rc = inflateEnd(&ctx->zstate);
  } else
  { rc = zwrite4(handle, NULL, 0, Z_FINISH);	/* flush */
    if ( rc == 0 )
      rc = deflateEnd(&ctx->zstate);
    else
      deflateEnd(&ctx->zstate);
  }

  switch(rc)
  { case Z_OK:
      DEBUG(1, Sdprintf("%s(): Z_OK\n",
		        (ctx->stream->flags & SIO_INPUT) ? "inflateEnd"
							 : "deflateEnd"));
      if ( ctx->close_parent )
      { IOSTREAM *parent = ctx->stream;
	free_zcontext(ctx);
	return Sclose(parent);
      } else
      { free_zcontext(ctx);
	return 0;
      }
    case Z_STREAM_ERROR:		/* inconsistent state */
    case Z_DATA_ERROR:			/* premature end */
    default:
      if ( ctx->close_parent )
      { IOSTREAM *parent = ctx->stream;
	free_zcontext(ctx);
	Sclose(parent);
	return -1;
      }

      free_zcontext(ctx);
      return -1;
  }
}


static IOFUNCTIONS zfunctions =
{ zread,
  zwrite,
  NULL,					/* seek */
  zclose,
  zcontrol,				/* zcontrol */
  NULL,					/* seek64 */
};


		 /*******************************
		 *	 PROLOG CONNECTION	*
		 *******************************/

#define COPY_FLAGS (SIO_INPUT|SIO_OUTPUT| \
		    SIO_TEXT| \
		    SIO_REPXML|SIO_REPPL|\
		    SIO_RECORDPOS)

static foreign_t
pl_zopen(term_t org, term_t new, term_t options)
{ term_t tail = PL_copy_term_ref(options);
  term_t head = PL_new_term_ref();
  z_context *ctx;
  zformat fmt = F_UNKNOWN;
  int level = Z_DEFAULT_COMPRESSION;
  IOSTREAM *s, *s2;
  int close_parent = TRUE;

  while(PL_get_list(tail, head, tail))
  { atom_t name;
    int arity;
    term_t arg = PL_new_term_ref();

    if ( !PL_get_name_arity(head, &name, &arity) || arity != 1 )
      return PL_type_error("option", head);
    _PL_get_arg(1, head, arg);

    if ( name == ATOM_format )
    { atom_t a;

      if ( !PL_get_atom_ex(arg, &a) )
	return FALSE;
      if ( a == ATOM_gzip )
	fmt = F_GZIP;
      else if ( a == ATOM_deflate )
	fmt = F_DEFLATE;
      else
	return PL_domain_error("compression_format", arg);
    } else if ( name == ATOM_level )
    { if ( !PL_get_integer_ex(arg, &level) )
	return FALSE;
      if ( level < 0 || level > 9 )
	return PL_domain_error("compression_level", arg);
    } else if ( name == ATOM_close_parent )
    { if ( !PL_get_bool_ex(arg, &close_parent) )
	return FALSE;
    }
  }
  if ( !PL_get_nil_ex(tail) )
    return FALSE;

  if ( !PL_get_stream_handle(org, &s) )
    return FALSE;			/* Error */
  ctx = alloc_zcontext(s);
  ctx->close_parent = close_parent;
  ctx->format = fmt;
  if ( (s->flags & SIO_OUTPUT) )
  { int rc;

    if ( fmt == F_GZIP )
    { rc = deflateInit2(&ctx->zstate, level, Z_DEFLATED, MAX_WBITS+16, MAX_MEM_LEVEL, 0);
    } else
    { rc = deflateInit(&ctx->zstate, level);
    }

    if ( rc != Z_OK )
    { free_zcontext(ctx);
      return FALSE;			/* TBD: Error */
    }
  }

  if ( !(s2 = Snew(ctx,
		   (s->flags&COPY_FLAGS)|SIO_FBUF,
		   &zfunctions))	)
  { free_zcontext(ctx);			/* no memory */

    return FALSE;
  }

  s2->encoding = s->encoding;
  ctx->zstream = s2;
  Sset_filter(s, s2);
  PL_release_stream(s);
  if ( PL_unify_stream(new, s2) )
  { return TRUE;
  } else
  { ctx->close_parent = FALSE;
    Sclose(s2);
    return PL_instantiation_error(new);	/* actually, over instantiation */
  }
}


#ifdef O_DEBUG
static foreign_t
zdebug(term_t level)
{ return PL_get_integer(level, &debuglevel);
}
#endif

		 /*******************************
		 *	       INSTALL		*
		 *******************************/

#define MKFUNCTOR(name, arity) PL_new_functor(PL_new_atom(name), arity)

install_t
install_zlib4pl(void)
{ ATOM_format       = PL_new_atom("format");
  ATOM_level        = PL_new_atom("level");
  ATOM_close_parent = PL_new_atom("close_parent");
  ATOM_gzip	    = PL_new_atom("gzip");
  ATOM_deflate	    = PL_new_atom("deflate");

  PL_register_foreign("zopen",  3, pl_zopen,  0);
#ifdef O_DEBUG
  PL_register_foreign("zdebug", 1, zdebug, 0);
#endif
}
