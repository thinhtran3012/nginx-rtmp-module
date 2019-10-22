/**
 * Use FFmpeg as a nginx-rtmp-module
 * TODO:
 * - Build FFmpeg as a nginx module
 * - Generate HLS/Mpeg DASH/fMP4 by FFmpeg from RTMP message
 * Advance Job:
 * - Multi worker
 * */

/**
 * Copyrigth (C) ducla@uiza.io
 * Created on 2019
 * */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_rtmp.h>
#include <ngx_rtmp_cmd_module.h>

/*FFmpeg's libraries*/
#include "libavutil/timestamp.h"
#include "libavformat/avformat.h"
#include "libavutil/opt.h"


static ngx_rtmp_publish_pt              next_publish;
static ngx_rtmp_close_stream_pt         next_close_stream;
static ngx_rtmp_stream_begin_pt         next_stream_begin;
static ngx_rtmp_stream_eof_pt           next_stream_eof;


static ngx_int_t ngx_rtmp_ffmpeg_postconfiguration(ngx_conf_t *cf);
static void * ngx_rtmp_ffmpeg_create_app_conf(ngx_conf_t *cf);
static char * ngx_rtmp_ffmpeg_merge_app_conf(ngx_conf_t *cf, void *parent, void *child);

typedef struct {
    ngx_str_t                           type; /*hls/mpeg dash/fmp4*/
    ngx_flag_t                          ffmpeg;
} ngx_rtmp_ffmpeg_app_conf_t;

typedef struct{
    ngx_str_t           stream_id;
} ngx_rtmp_ffmpeg_ctx_t;

static ngx_command_t ngx_rtmp_ffmpeg_commands[] = {
    {
        ngx_string("ffmpeg"),
        NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_flag_slot,
        NGX_RTMP_APP_CONF_OFFSET,
        offsetof(ngx_rtmp_ffmpeg_app_conf_t, ffmpeg),
        NULL
    },
    {
        ngx_string("type"),
        NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_RTMP_APP_CONF_OFFSET,
        offsetof(ngx_rtmp_ffmpeg_app_conf_t, type),
        NULL
    },
    ngx_null_command
};

static ngx_rtmp_module_t ngx_rtmp_ffmpeg_module_ctx = {
    NULL,                               /* preconfiguration */
    ngx_rtmp_ffmpeg_postconfiguration,     /* postconfiguration */

    NULL,                               /* create main configuration */
    NULL,                               /* init main configuration */

    NULL,                               /* create server configuration */
    NULL,                               /* merge server configuration */

    ngx_rtmp_ffmpeg_create_app_conf,       /* create location configuration */
    ngx_rtmp_ffmpeg_merge_app_conf,        /* merge location configuration */
};

ngx_module_t  ngx_rtmp_ffmpeg_module = {
    NGX_MODULE_V1,
    &ngx_rtmp_ffmpeg_module_ctx,           /* module context */
    ngx_rtmp_ffmpeg_commands,              /* module directives */
    NGX_RTMP_MODULE,                    /* module type */
    NULL,                               /* init master */
    NULL,                               /* init module */
    NULL,                               /* init process */
    NULL,                               /* init thread */
    NULL,                               /* exit thread */
    NULL,                               /* exit process */
    NULL,                               /* exit master */
    NGX_MODULE_V1_PADDING
};

static ngx_int_t
ngx_rtmp_ffmpeg_video(ngx_rtmp_session_t *s, ngx_rtmp_header_t *h,
    ngx_chain_t *in)
{
    return NGX_OK;
}

static ngx_int_t
ngx_rtmp_ffmpeg_audio(ngx_rtmp_session_t *s, ngx_rtmp_header_t *h,
    ngx_chain_t *in)
{
    return NGX_OK;
}

static ngx_int_t
ngx_rtmp_ffmpeg_publish(ngx_rtmp_session_t *s, ngx_rtmp_publish_t *v)
{
    ngx_rtmp_ffmpeg_app_conf_t          *facf;
    ngx_rtmp_ffmpeg_ctx_t               *ctx;
    AVFormatContext                     *av_out_format_context = NULL;
    AVPacket                            *av_out_packet;
    AVCodec                             *av_out_codec;
    AVCodecContext                      *av_out_codec_context;
    AVOutputFormat                      *av_output_format;


    facf = ngx_rtmp_get_module_app_conf(s, ngx_rtmp_ffmpeg_module);
    //ignore if this module is not enable
    if (facf == NULL || !facf->ffmpeg) {
        goto next;
    }
    ngx_log_debug2(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                   "ffmpeg: publish: name='%s' type='%s'",
                   v->name, v->type);

    ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_ffmpeg_module);
    if (ctx == NULL) {
        ctx = ngx_pcalloc(s->connection->pool, sizeof(ngx_rtmp_ffmpeg_ctx_t));
        ngx_rtmp_set_ctx(s, ctx, ngx_rtmp_ffmpeg_module);
        ctx->stream_id.len = ngx_strlen(v->name);
        ctx->stream_id.data = ngx_palloc(s->connection->pool, ctx->stream_id.len + 1);
        if (ctx->stream_id.data == NULL) {
            return NGX_ERROR;
        }
        *ngx_cpymem(ctx->stream_id.data, v->name, ctx->stream_id.len) = 0;
    }

    //need to init ffmpeg's parameters
    av_output_format = av_guess_format("hls", NULL, NULL);
    if(!av_output_format){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                      "ffmpeg: no output format");
        return NGX_ERROR;
    }

    next:
    return next_publish(s, v);
}

static ngx_int_t
ngx_rtmp_ffmpeg_close_stream(ngx_rtmp_session_t *s, ngx_rtmp_close_stream_t *v)
{
    return next_close_stream(s, v);
}

static ngx_int_t
ngx_rtmp_ffmpeg_stream_begin(ngx_rtmp_session_t *s, ngx_rtmp_stream_begin_t *v)
{
    return next_stream_begin(s, v);
}

static ngx_int_t
ngx_rtmp_ffmpeg_stream_eof(ngx_rtmp_session_t *s, ngx_rtmp_stream_eof_t *v)
{
    return next_stream_eof(s, v);
}

static ngx_int_t ngx_rtmp_ffmpeg_postconfiguration(ngx_conf_t *cf)
{
    ngx_rtmp_core_main_conf_t   *cmcf;
    ngx_rtmp_handler_pt         *h;

    cmcf = ngx_rtmp_conf_get_module_main_conf(cf, ngx_rtmp_core_module);
    
    /*For video messages*/
    h = ngx_array_push(&cmcf->events[NGX_RTMP_MSG_VIDEO]);
    *h = ngx_rtmp_ffmpeg_video;

    /*For audio messages*/
    h = ngx_array_push(&cmcf->events[NGX_RTMP_MSG_AUDIO]);
    *h = ngx_rtmp_ffmpeg_audio;

    next_publish = ngx_rtmp_publish;
    ngx_rtmp_publish = ngx_rtmp_ffmpeg_publish;

    next_close_stream = ngx_rtmp_close_stream;
    ngx_rtmp_close_stream = ngx_rtmp_ffmpeg_close_stream;

    next_stream_begin = ngx_rtmp_stream_begin;
    ngx_rtmp_stream_begin = ngx_rtmp_ffmpeg_stream_begin;

    next_stream_eof = ngx_rtmp_stream_eof;
    ngx_rtmp_stream_eof = ngx_rtmp_ffmpeg_stream_eof;

    return NGX_OK;
}

static void *
ngx_rtmp_ffmpeg_create_app_conf(ngx_conf_t *cf)
{
    ngx_rtmp_ffmpeg_app_conf_t *conf;
    conf = ngx_pcalloc(cf->pool, sizeof(ngx_rtmp_ffmpeg_app_conf_t));
    if (conf == NULL) {
        return NULL;
    }
    conf->ffmpeg = NGX_CONF_UNSET;
    return conf;
}

static char *
ngx_rtmp_ffmpeg_merge_app_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_rtmp_ffmpeg_app_conf_t    *prev = parent;
    ngx_rtmp_ffmpeg_app_conf_t    *conf = child;
    ngx_conf_merge_value(conf->ffmpeg, prev->ffmpeg, 0);
    ngx_conf_merge_str_value(conf->type, prev->type, "hls");//default we use hls format

    return NGX_CONF_OK;
}