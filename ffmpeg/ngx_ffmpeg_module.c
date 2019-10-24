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
#include <ngx_rtmp_codec_module.h>

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
    ngx_str_t                           path; /*Path for saving data*/
} ngx_rtmp_ffmpeg_app_conf_t;

typedef struct{
    ngx_str_t                           stream_id;
    ngx_str_t                           playlist;//path to playlist
    AVFormatContext                     *out_av_format_context;
    AVPacket                            *out_av_packet;
    AVCodec                             *out_av_codec;
    AVCodecContext                      *out_av_codec_context;
    int                                 is_codec_opened;
    int                                 is_video_stream_opened;
    int                                 video_stream_index;
    AVOutputFormat                      *out_av_format;
    int                                 nb_streams;//number of stream in file
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
    {
        ngx_string("path"),
        NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_RTMP_APP_CONF_OFFSET,
        offsetof(ngx_rtmp_ffmpeg_app_conf_t, path),
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
    ngx_rtmp_ffmpeg_app_conf_t        *facf;
    ngx_rtmp_ffmpeg_ctx_t             *ctx;
    ngx_rtmp_codec_ctx_t              *codec_ctx;
    uint8_t                           ftype, htype;
    int                               ret;
    u_char                            *p;
    static u_char                     buffer[1024 * 1024];
    size_t                            size, bsize;
    AVFrame                           *frame;
    int                               got_frame = 0;

    facf = ngx_rtmp_get_module_app_conf(s, ngx_rtmp_ffmpeg_module);
    ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_ffmpeg_module);
    codec_ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_codec_module);    
    if (facf == NULL || !facf->ffmpeg || ctx == NULL || codec_ctx == NULL ||
        codec_ctx->avc_header == NULL || h->mlen < 5)
    {
        return NGX_OK;
    }    

    /* Only H264 is supported */
    if (codec_ctx->video_codec_id != NGX_RTMP_VIDEO_H264) {
        return NGX_OK;
    }

    //video is always in track 0    
    if(!ctx->is_video_stream_opened){
        AVStream *out_av_stream;
        out_av_stream = avformat_new_stream(ctx->out_av_format_context, NULL);        
        if(!out_av_stream){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not create video stream");
            return NGX_ERROR;
        }        
        ctx->is_video_stream_opened = 1;
        ctx->nb_streams += 1;
        ctx->video_stream_index = ctx->nb_streams;
    }    
    if(!ctx->is_codec_opened){
        ctx->out_av_codec = avcodec_find_encoder(AV_CODEC_ID_H264);        
        if(!ctx->out_av_codec){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not find approriate encoder.");
            return NGX_ERROR;
        }
        ctx->out_av_codec_context = avcodec_alloc_context3(ctx->out_av_codec);
        if(!ctx->out_av_codec_context){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not allocate output codec context.");
            return NGX_ERROR;
        }

        ctx->out_av_codec_context->bit_rate = 40000;
        ctx->out_av_codec_context->width = codec_ctx->width;
        ctx->out_av_codec_context->height = codec_ctx->height;
        ctx->out_av_codec_context->time_base = (AVRational){1, codec_ctx->frame_rate};
        ctx->out_av_codec_context->gop_size = 10;
        ctx->out_av_codec_context->max_b_frames = 1;
        ctx->out_av_codec_context->pix_fmt = AV_PIX_FMT_YUV420P;
        ret = avcodec_open2(ctx->out_av_codec_context, ctx->out_av_codec, NULL);
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not open output codec context.");
            return NGX_ERROR;
        }
        ret = av_opt_set(ctx->out_av_format_context->priv_data, "hls_segment_type", "fmp4", 0); 
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not set options.");
            return NGX_ERROR;
        }
        AVDictionary* opts = NULL;
        ret = avformat_write_header(ctx->out_av_format_context, &opts);
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not write header.");
            // return NGX_ERROR;
        }
        ctx->is_codec_opened = 1;
    }    
    if (in->buf->last - in->buf->pos < 5) {
        return NGX_ERROR;
    }
        
    //how to decode this data?
    p = buffer;
    size = 0;

    for (; in && size < sizeof(buffer); in = in->next) {

        bsize = (size_t) (in->buf->last - in->buf->pos);
        if (size + bsize > sizeof(buffer)) {
            bsize = (size_t) (sizeof(buffer) - size);
        }

        p = ngx_cpymem(p, in->buf->pos, bsize);
        size += bsize;
    }
    AVPacket *pkt;
    // av_init_packet(pkt);
    pkt = av_packet_alloc();
    if(!pkt){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not alloc packet.");
        return NGX_ERROR;
    }
    pkt->data = (uint8_t *)p;
    pkt->size = size;
    frame = av_frame_alloc();
    if(!frame){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not alloc frame.");
        return NGX_ERROR;
    }
    ret = avcodec_decode_video2(ctx->out_av_codec_context, frame, &got_frame, pkt);
    if(ret < 0){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not got frame %s \n", av_err2str(ret));
    }else{
        if(got_frame){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: it's a frame.");
        }else{
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: it's not a frame.");
        }
    }    
    if(!pkt){
        av_packet_free(&pkt);
    }
    if(!frame){
        av_frame_free(&frame);
    }
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
    u_char                              *p;
    int                                 ret;


    facf = ngx_rtmp_get_module_app_conf(s, ngx_rtmp_ffmpeg_module);    
    //ignore if this module is not enable
    if (facf == NULL || !facf->ffmpeg || facf->path.len == 0) {
        goto next;
    }
    ngx_log_debug2(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                   "ffmpeg: publish: name='%s' type='%s'",
                   v->name, v->type);    
    ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_ffmpeg_module);
    if (ctx == NULL) {
        ctx = ngx_pcalloc(s->connection->pool, sizeof(ngx_rtmp_ffmpeg_ctx_t));
        ngx_rtmp_set_ctx(s, ctx, ngx_rtmp_ffmpeg_module);        
    }    
    ctx->stream_id.len = ngx_strlen(v->name);
    ctx->stream_id.data = ngx_palloc(s->connection->pool, ctx->stream_id.len + 1);
    if (ctx->stream_id.data == NULL) {
        return NGX_ERROR;
    }
    *ngx_cpymem(ctx->stream_id.data, v->name, ctx->stream_id.len) = 0;
    //get path to write data
    // path/stream_id
    ctx->playlist.len = facf->path.len + 1 + ctx->stream_id.len + sizeof("/index.m3u8");
    ctx->playlist.data = ngx_palloc(s->connection->pool, ctx->playlist.len + 1);
    p = ngx_cpymem(ctx->playlist.data, facf->path.data, facf->path.len);
    if (p[-1] != '/') {
        *p++ = '/';
    }
    p = ngx_cpymem(p, ctx->stream_id.data, ctx->stream_id.len);
    p = ngx_cpymem(p, "/index.m3u8", sizeof("/index.m3u8") - 1);
    ctx->playlist.len = p - ctx->playlist.data;
    *p = 0;
    //need to init ffmpeg's parameters
    if(!ctx->out_av_format_context){
        ctx->out_av_format_context = NULL;
        avformat_alloc_output_context2(&(ctx->out_av_format_context), NULL, NULL, (const char*)ctx->playlist.data);
        if(!ctx->out_av_format_context){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Could not create output format context.");
            return NGX_ERROR;
        }
        ctx->nb_streams = -1;
    }    
    ret = av_opt_set(ctx->out_av_format_context->priv_data, "hls_segment_type", "mpegts", 0);  
    if(ret < 0){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: %s.", av_err2str(ret));        
        return NGX_ERROR;
    }
    ctx->out_av_format = av_guess_format("hls", NULL, NULL);
    if(!ctx->out_av_format){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: no output format.");
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
    ngx_conf_merge_str_value(conf->path, prev->path, "");

    return NGX_CONF_OK;
}