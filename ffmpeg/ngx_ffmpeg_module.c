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

#define NGX_RTMP_FFMPEG_DIR_ACCESS        0744
#define NGX_RTMP_FFMPEG_BUFSIZE           (1024*1024)

static ngx_int_t ngx_rtmp_ffmpeg_postconfiguration(ngx_conf_t *cf);
static void * ngx_rtmp_ffmpeg_create_app_conf(ngx_conf_t *cf);
static char * ngx_rtmp_ffmpeg_merge_app_conf(ngx_conf_t *cf, void *parent, void *child);
static ngx_int_t ngx_rtmp_ffmpeg_copy(ngx_rtmp_session_t *s, void *dst, u_char **src, size_t n, ngx_chain_t **in);
static ngx_int_t ngx_rtmp_ffmpeg_append_sps_pps(ngx_rtmp_session_t *s, ngx_buf_t *out);
static ngx_int_t ngx_rtmp_ffmpeg_append_aud(ngx_rtmp_session_t *s, ngx_buf_t *out);

typedef struct {
    ngx_str_t                           type; /*hls/mpeg dash/fmp4*/
    ngx_flag_t                          ffmpeg;
    ngx_str_t                           path; /*Path for saving data*/
    ngx_flag_t                          nested;/*Nested directory*/
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
    int64_t                             pre_timestamp;
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
        ngx_string("ffmpeg_path"),
        NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_RTMP_APP_CONF_OFFSET,
        offsetof(ngx_rtmp_ffmpeg_app_conf_t, path),
        NULL
    },
    { ngx_string("ffmpeg_nested"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_ffmpeg_app_conf_t, nested),
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
    uint8_t                           fmt, ftype, htype, nal_type, src_nal_type;
    int                               ret;
    u_char                            *p;
    static u_char                     buffer[NGX_RTMP_FFMPEG_BUFSIZE];
    size_t                            size, bsize;
    AVFrame                           *frame;
    int                               got_frame = 0;
    AVStream                          *out_av_stream;
    ngx_buf_t                         out, *b;
    uint32_t                          cts;
    ngx_uint_t                        nal_bytes;
    ngx_int_t                         aud_sent, sps_pps_sent, boundary;
    uint32_t                          len, rlen;

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
        ret = av_opt_set(ctx->out_av_codec_context->priv_data, "preset", "slow", 0);  
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not set output codec context priv data %s.", av_err2str(ret));
            return NGX_ERROR;
        }
        ret = avcodec_open2(ctx->out_av_codec_context, ctx->out_av_codec, NULL);
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not open output codec context.");
            return NGX_ERROR;
        }
        ret = avcodec_parameters_from_context(out_av_stream->codecpar, ctx->out_av_codec_context);
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not set parameters %s.", av_err2str(ret));
            return NGX_ERROR;
        }
        // ret = av_opt_set(ctx->out_av_format_context->priv_data, "hls_segment_type", "fmp4", 0); 
        // if(ret < 0){
        //     ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not set options.");
        //     return NGX_ERROR;
        // }
        if(!(ctx->out_av_format_context->oformat->flags & AVFMT_NOFILE)){
            ret = avio_open(&(ctx->out_av_format_context->pb), ctx->playlist.data, AVIO_FLAG_WRITE);
            if(ret < 0){
                ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not open output file %s\n", av_err2str(ret));
                return NGX_ERROR;
            }
        }
        AVDictionary* opts = NULL;
        ret = avformat_write_header(ctx->out_av_format_context, &opts);
        if(ret < 0){
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: could not write header %s\n", av_err2str(ret));
            return NGX_ERROR;
        }
        ctx->is_codec_opened = 1;
    }    
    if (in->buf->last - in->buf->pos < 5) {
        return NGX_ERROR;
    }
        
    //how to decode this data?
    p = in->buf->pos;
    if (ngx_rtmp_ffmpeg_copy(s, &fmt, &p, 1, &in) != NGX_OK) {
        return NGX_ERROR;
    }
    ftype = (fmt & 0xf0) >> 4;

    if (ngx_rtmp_ffmpeg_copy(s, &htype, &p, 1, &in) != NGX_OK) {
        return NGX_ERROR;
    }

    if (htype != 1) {
        return NGX_OK;
    }

    /* 3 bytes: decoder delay */

    if (ngx_rtmp_ffmpeg_copy(s, &cts, &p, 3, &in) != NGX_OK) {
        return NGX_ERROR;
    }

    cts = ((cts & 0x00FF0000) >> 16) | ((cts & 0x000000FF) << 16) |
          (cts & 0x0000FF00);

    ngx_memzero(&out, sizeof(out));

    out.start = buffer;
    out.end = buffer + sizeof(buffer);
    out.pos = out.start;
    out.last = out.pos;

    nal_bytes = codec_ctx->avc_nal_bytes;
    aud_sent = 0;
    sps_pps_sent = 0;

    while (in) {
        //calculate length of payload
        if (ngx_rtmp_ffmpeg_copy(s, &rlen, &p, nal_bytes, &in) != NGX_OK) {
            return NGX_OK;
        }

        len = 0;
        ngx_rtmp_rmemcpy(&len, &rlen, nal_bytes);
        if (len == 0) {
            continue;
        }
        //get nal type
        if (ngx_rtmp_ffmpeg_copy(s, &src_nal_type, &p, 1, &in) != NGX_OK) {
            return NGX_OK;
        }
        nal_type = src_nal_type & 0x1f;
        //ignore if this is SPS/PPS/AUD Unit
        if (nal_type >= 7 && nal_type <= 9) {
            if (ngx_rtmp_ffmpeg_copy(s, NULL, &p, len - 1, &in) != NGX_OK) {
                return NGX_ERROR;
            }
            continue;
        }
        if (!aud_sent) {
            switch (nal_type) {
                case 1:
                case 5:
                case 6:
                    if (ngx_rtmp_ffmpeg_append_aud(s, &out) != NGX_OK) {
                        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                                      "ffmpeg: error appending AUD NAL");
                    }
                    /* fall through */
                case 9:
                    aud_sent = 1;
                    break;
            }
        }
        switch (nal_type) {
            case 1:
                sps_pps_sent = 0;
                break;
            case 5:
                if (sps_pps_sent) {
                    break;
                }
                if (ngx_rtmp_ffmpeg_append_sps_pps(s, &out) != NGX_OK) {
                    ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                                  "ffmpeg: error appenging SPS/PPS NALs");
                }
                sps_pps_sent = 1;
                break;
        }
        //NOTE: we only read encode body, do not insert any other data
        if (out.end - out.last < (ngx_int_t) len) {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                          "ffmpeg: not enough buffer for NAL");
            return NGX_OK;
        }
        if (ngx_rtmp_ffmpeg_copy(s, out.last, &p, len - 1, &in) != NGX_OK) {
            return NGX_ERROR;
        }

        out.last += (len - 1);
    }
    //now we have out is a H264 frame ~ FFmpeg Packet
    AVPacket *pkt;
    // av_init_packet(pkt);
    pkt = av_packet_alloc();
    if(!pkt){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not alloc packet.");
        return NGX_ERROR;
    }
    if(!ctx->pre_timestamp){
        ctx->pre_timestamp = (int64_t)h->timestamp;
    }
    pkt->data = (uint8_t *)out.start;
    pkt->size = (out.last - out.start);     
    pkt->pts = (int64_t) h->timestamp;       
    pkt->dts = pkt->pts;    
    pkt->flags |= AV_PKT_FLAG_KEY; 
    pkt->stream_index = 0;
    
    if(ctx->pre_timestamp){
        pkt->duration = pkt->pts - ctx->pre_timestamp;
    }
    ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: packet duration %d.", pkt->duration);
    // ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: message size: %d.", size);
    // frame = av_frame_alloc();
    // if(!frame){
    //     ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not alloc frame.");
    //     return NGX_ERROR;
    // }
    ret = avcodec_send_packet(ctx->out_av_codec_context, pkt);
    if(ret < 0){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not send packet to decoder %s \n", av_err2str(ret));
        // return NGX_ERROR;
    }else{
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: decode success.");
    }
    ret = av_interleaved_write_frame(ctx->out_av_format_context, pkt);
    if(ret < 0){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: Can not write data %s.", av_err2str(ret));
        return NGX_ERROR;
    }    
    av_packet_unref(pkt);
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

/**
 * Make sure data directory is exist before writting data
 **/
static ngx_int_t
ngx_rtmp_ffmpeg_ensure_directory(ngx_rtmp_session_t *s)
{
    static u_char               path[NGX_MAX_PATH + 1];
    ngx_rtmp_ffmpeg_app_conf_t  *facf;
    ngx_file_info_t             fi;
    ngx_rtmp_ffmpeg_ctx_t       *ctx;
    size_t                      len;

    facf = ngx_rtmp_get_module_app_conf(s, ngx_rtmp_ffmpeg_module);
    *ngx_snprintf(path, sizeof(path) - 1, "%V", &facf->path) = 0;
    if (ngx_file_info(path, &fi) == NGX_FILE_ERROR) {
        if (ngx_errno != NGX_ENOENT) {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, ngx_errno,
                          "ffmpeg: " ngx_file_info_n " failed on '%V'",
                          &facf->path);
            return NGX_ERROR;
        }
        /* ENOENT */

        if (ngx_create_dir(path, NGX_RTMP_FFMPEG_DIR_ACCESS) == NGX_FILE_ERROR) {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, ngx_errno,
                          "ffmpeg: " ngx_create_dir_n " failed on '%V'",
                          &facf->path);
            return NGX_ERROR;
        }

        ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                       "ffmpeg: directory '%V' created", &facf->path);
    }else{
        if (!ngx_is_dir(&fi)) {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                          "ffmpeg: '%V' exists and is not a directory",
                          &facf->path);
            return  NGX_ERROR;
        }

        ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                       "ffmpeg: directory '%V' exists", &facf->path);
    }
    if (!facf->nested) {
        return NGX_OK;
    }
    ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_ffmpeg_module);
    len = facf->path.len;
    if (facf->path.data[len - 1] == '/') {
        len--;
    }
    *ngx_snprintf(path, sizeof(path) - 1, "%*s/%V", len, facf->path.data,
                  &ctx->stream_id) = 0;
    //check if nested directory is exist
    if (ngx_file_info(path, &fi) != NGX_FILE_ERROR) {
        if (ngx_is_dir(&fi)) {
            ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                           "ffmpeg: directory '%s' exists", path);
            return NGX_OK;
        }
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                      "ffmpeg: '%s' exists and is not a directory", path);

        return  NGX_ERROR;
    }
    if (ngx_errno != NGX_ENOENT) {
        ngx_log_error(NGX_LOG_ERR, s->connection->log, ngx_errno,
                      "ffmpeg: " ngx_file_info_n " failed on '%s'", path);
        return NGX_ERROR;
    }
    //create nested directory if need
    if (ngx_create_dir(path, NGX_RTMP_FFMPEG_DIR_ACCESS) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_ERR, s->connection->log, ngx_errno,
                      "ffmpeg: " ngx_create_dir_n " failed on '%s'", path);
        return NGX_ERROR;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                   "ffmpeg: directory '%s' created", path);

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
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: %s.", ctx->playlist.data);
        avformat_alloc_output_context2(&(ctx->out_av_format_context), NULL, NULL, ctx->playlist.data);
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
    ret = av_opt_set(ctx->out_av_format_context->priv_data, "hls_time", "5", 0);  
    if(ret < 0){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: %s.", av_err2str(ret));        
        return NGX_ERROR;
    }
    // ret = av_opt_set(ctx->out_av_format_context->priv_data, "hls_segment_filename", "%d.ts", 0);  
    // if(ret < 0){
    //     ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: %s.", av_err2str(ret));        
    //     return NGX_ERROR;
    // }
    ctx->out_av_format = av_guess_format("hls", NULL, NULL);
    if(!ctx->out_av_format){
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: no output format.");
        return NGX_ERROR;
    }
    ctx->out_av_format_context->oformat = ctx->out_av_format;
    if (ngx_rtmp_ffmpeg_ensure_directory(s) != NGX_OK) {
        return NGX_ERROR;
    }    
    next:
    return next_publish(s, v);
}

static ngx_int_t
ngx_rtmp_ffmpeg_close_stream(ngx_rtmp_session_t *s, ngx_rtmp_close_stream_t *v)
{
    ngx_rtmp_ffmpeg_ctx_t       *ctx;
    ngx_rtmp_ffmpeg_app_conf_t  *facf;

    facf = ngx_rtmp_get_module_app_conf(s, ngx_rtmp_ffmpeg_module);

    ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_ffmpeg_module);

    if (facf == NULL || !facf->ffmpeg || ctx == NULL) {
        goto next;
    }
    // ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: 1.");
    av_write_trailer(ctx->out_av_format_context);    
    // ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "ffmpeg: 2.");
    
    next:
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

/**
 * @param dst where copy to. If NULL, we only read, and move pointer src
 * @param src where copy from
 * @param n number of byte to copy
 * @param in nginx buffer
 **/ 
static ngx_int_t
ngx_rtmp_ffmpeg_copy(ngx_rtmp_session_t *s, void *dst, u_char **src, size_t n,
    ngx_chain_t **in)
{
    u_char  *last;
    size_t   pn;

    if (*in == NULL) {
        return NGX_ERROR;
    }

    for ( ;; ) {
        last = (*in)->buf->last;

        if ((size_t)(last - *src) >= n) {
            if (dst) {
                ngx_memcpy(dst, *src, n);
            }

            *src += n;

            while (*in && *src == (*in)->buf->last) {
                *in = (*in)->next;
                if (*in) {
                    *src = (*in)->buf->pos;
                }
            }

            return NGX_OK;
        }

        pn = last - *src;

        if (dst) {
            ngx_memcpy(dst, *src, pn);
            dst = (u_char *)dst + pn;
        }

        n -= pn;
        *in = (*in)->next;

        if (*in == NULL) {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                          "ffmpeg: failed to read %uz byte(s)", n);
            return NGX_ERROR;
        }

        *src = (*in)->buf->pos;
    }
}

static ngx_int_t
ngx_rtmp_ffmpeg_append_sps_pps(ngx_rtmp_session_t *s, ngx_buf_t *out)
{
    ngx_rtmp_codec_ctx_t           *codec_ctx;
    u_char                         *p;
    ngx_chain_t                    *in;
    ngx_rtmp_ffmpeg_ctx_t          *ctx;
    int8_t                         nnals;
    uint16_t                       len, rlen;
    ngx_int_t                      n;

    ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_ffmpeg_module);

    codec_ctx = ngx_rtmp_get_module_ctx(s, ngx_rtmp_codec_module);

    if (ctx == NULL || codec_ctx == NULL) {
        return NGX_ERROR;
    }

    in = codec_ctx->avc_header;
    if (in == NULL) {
        return NGX_ERROR;
    }

    p = in->buf->pos;

    /*
     * Skip bytes:
     * - flv fmt
     * - H264 CONF/PICT (0x00)
     * - 0
     * - 0
     * - 0
     * - version
     * - profile
     * - compatibility
     * - level
     * - nal bytes
     */

    if (ngx_rtmp_ffmpeg_copy(s, NULL, &p, 10, &in) != NGX_OK) {
        return NGX_ERROR;
    }

    /* number of SPS NALs */
    if (ngx_rtmp_ffmpeg_copy(s, &nnals, &p, 1, &in) != NGX_OK) {
        return NGX_ERROR;
    }

    nnals &= 0x1f; /* 5lsb */

    ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                   "ffmpeg: SPS number: %uz", nnals);

    /* SPS */
    for (n = 0; ; ++n) {
        for (; nnals; --nnals) {

            /* NAL length */
            if (ngx_rtmp_ffmpeg_copy(s, &rlen, &p, 2, &in) != NGX_OK) {
                return NGX_ERROR;
            }

            ngx_rtmp_rmemcpy(&len, &rlen, 2);

            ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                           "ffmpeg: header NAL length: %uz", (size_t) len);

            /* AnnexB prefix */
            if (out->end - out->last < 4) {
                ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                              "ffmpeg: too small buffer for header NAL size");
                return NGX_ERROR;
            }

            *out->last++ = 0;
            *out->last++ = 0;
            *out->last++ = 0;
            *out->last++ = 1;

            /* NAL body */
            if (out->end - out->last < len) {
                ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                              "ffmpeg: too small buffer for header NAL");
                return NGX_ERROR;
            }

            if (ngx_rtmp_ffmpeg_copy(s, out->last, &p, len, &in) != NGX_OK) {
                return NGX_ERROR;
            }

            out->last += len;
        }

        if (n == 1) {
            break;
        }

        /* number of PPS NALs */
        if (ngx_rtmp_ffmpeg_copy(s, &nnals, &p, 1, &in) != NGX_OK) {
            return NGX_ERROR;
        }

        ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                       "ffmpeg: PPS number: %uz", nnals);
    }

    return NGX_OK;
}


static ngx_int_t
ngx_rtmp_ffmpeg_append_aud(ngx_rtmp_session_t *s, ngx_buf_t *out)
{
    static u_char   aud_nal[] = { 0x00, 0x00, 0x00, 0x01, 0x09, 0xf0 };

    if (out->last + sizeof(aud_nal) > out->end) {
        return NGX_ERROR;
    }

    out->last = ngx_cpymem(out->last, aud_nal, sizeof(aud_nal));

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
    conf->nested = NGX_CONF_UNSET;
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
    ngx_conf_merge_value(conf->nested, prev->nested, 1);

    return NGX_CONF_OK;
}