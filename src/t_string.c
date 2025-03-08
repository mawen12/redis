/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include <math.h> /* isnan(), isinf() */

/* Forward declarations */
int getGenericCommand(client *c);

/*-----------------------------------------------------------------------------
 * String 命令集合
 *----------------------------------------------------------------------------*/

/**
 * 检查添加长度后的字符串长度是否超过了限制
 * 
 * @param c 回写错误信息
 * @param size 当前大小
 * @param append 追加大小
 * @retval C_OK 
 */
static int checkStringLength(client *c, long long size, long long append) {
    if (mustObeyClient(c))
        return C_OK;
    /* 'uint64_t' cast is there just to prevent undefined behavior on overflow */
    long long total = (uint64_t)size + append;
    /* Test configured max-bulk-len represending a limit of the biggest string object,
     * and also test for overflow. */
    if (total > server.proto_max_bulk_len || total < size || total < append) {
        addReplyError(c,"string exceeds maximum allowed size (proto-max-bulk-len)");
        return C_ERR;
    }
    return C_OK;
}

/**
 * setGenericCommand() 函数实现了带有不同选项和变体的SET操作。
 * 该函数被调用是为了实现以下命令：SET, SETNX, PSETEX, SETNX, GETSET。
 * 
 * 'flags' 修改了命令（NX, XX or GET）的行为
 * 
 * 'expire' 代表以用户传递的Redis对象形式设置的过期时间。
 * 它根据指定的'unit'进行解释。
 * 
 * 'ok_reply' 和 'abort_reply' 是函数操作执行后（或因为NX/XX而未执行）返回给客户端的答复。
 * 
 * 如果 ok_reply 为空，则会使用 "+OK"。
 * 如果 abort_reply 为空，则会使用 "$-1"。
 * 
 * 原始的命令格式为：SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
 * 
 * 
 * +---------+------+------+---------+---------+----+----+----+----+
 * | PERSIST | PXAT | EXAT | SET_GET | KEEPTTL | PX | EX | XX | NX |
 * +---------+------+------+---------+---------+----+----+----+----+
 * |       1 |    1 |    1 |       1 |       1 |  1 |  1 |  1 |  1 |
 * +---------+------+------+---------+---------+----+----+----+----+
 */
#define OBJ_NO_FLAGS 0             /* 没有任何表示，就是简单的 SET key value */
#define OBJ_SET_NX (1<<0)          /* KEY必须不存在，即 SET key value NX */
#define OBJ_SET_XX (1<<1)          /* KEY必须已存在，即 SET key value XX */
#define OBJ_EX (1<<2)              /* 过期时间为秒，即 SET key value EX <seconds> */
#define OBJ_PX (1<<3)              /* 过期时间为毫秒，即 SET key value PX <milliseconds> */
#define OBJ_KEEPTTL (1<<4)         /* 保留TTL，即 SET key value KEEPTTL */
#define OBJ_SET_GET (1<<5)         /* 设置前返回之前值，即 SET key value GET */
#define OBJ_EXAT (1<<6)            /* 设置过期时间为unix-time秒，即 SET key value EXAT unix-time-seconds */
#define OBJ_PXAT (1<<7)            /* 设置过期时间为unix-time毫秒，即 SET key value PXAT unix-time-milliseconds */
#define OBJ_PERSIST (1<<8)         /* 设置持久保存，异常过期，即 SET key value PERSIST */

/* Forward declaration */
static int getExpireMillisecondsOrReply(client *c, robj *expire, int flags, int unit, long long *milliseconds);

/**
 * 通用的SET命令
 */
void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* initialized to avoid any harmness warning */
    int found = 0;
    int setkey_flags = 0;
    
    // 设置了过期，计算过期的绝对时间戳，保存到"millseconds"
    if (expire && getExpireMillisecondsOrReply(c, expire, flags, unit, &milliseconds) != C_OK) {
        return;
    }

    // 命令为SET key value GET
    if (flags & OBJ_SET_GET) {
        // 如果GET操作因为键类型不匹配，则直接返回
        if (getGenericCommand(c) == C_ERR) return;
    }

    // 查找结果
    found = (lookupKeyWrite(c->db,key) != NULL);

    if ((flags & OBJ_SET_NX && found) ||
        (flags & OBJ_SET_XX && !found))
    {
        // 命令为SET key value NX，且Key已经存在，或命令为SET key value XX，且key不存在时的处理
        if (!(flags & OBJ_SET_GET)) {
            // 命令不是 SET key value GET时，添加回复。
            addReply(c, abort_reply ? abort_reply : shared.null[c->resp]);
        }
        return;
    }

    // 如果设置了 expire，我们应该避免删除TTL以便之后能够更新，而不是先删除再创建
    setkey_flags |= ((flags & OBJ_KEEPTTL) || expire) ? SETKEY_KEEPTTL : 0;
    // 根据是否找到key，则设置对应标识
    setkey_flags |= found ? SETKEY_ALREADY_EXIST : SETKEY_DOESNT_EXIST;
    // 
    setKey(c,c->db,key,val,setkey_flags);
    server.dirty++;
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);

    if (expire) {
        setExpire(c,c->db,key,milliseconds);
        /* Propagate as SET Key Value PXAT millisecond-timestamp if there is
         * EX/PX/EXAT flag. */
        if (!(flags & OBJ_PXAT)) {
            robj *milliseconds_obj = createStringObjectFromLongLong(milliseconds);
            rewriteClientCommandVector(c, 5, shared.set, key, val, shared.pxat, milliseconds_obj);
            decrRefCount(milliseconds_obj);
        }
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
    }

    if (!(flags & OBJ_SET_GET)) {
        addReply(c, ok_reply ? ok_reply : shared.ok);
    }

    /* Propagate without the GET argument (Isn't needed if we had expire since in that case we completely re-written the command argv) */
    if ((flags & OBJ_SET_GET) && !expire) {
        int argc = 0;
        int j;
        robj **argv = zmalloc((c->argc-1)*sizeof(robj*));
        for (j=0; j < c->argc; j++) {
            char *a = c->argv[j]->ptr;
            /* Skip GET which may be repeated multiple times. */
            if (j >= 3 &&
                (a[0] == 'g' || a[0] == 'G') &&
                (a[1] == 'e' || a[1] == 'E') &&
                (a[2] == 't' || a[2] == 'T') && a[3] == '\0')
                continue;
            argv[argc++] = c->argv[j];
            incrRefCount(c->argv[j]);
        }
        replaceClientCommandVector(c, argc, argv);
    }
}

/**
 * 提取给定的SET/GET命令中的expire相关参数（EX/PX/EXAT/PXAT）
 * 
 * @param client 发送expire参数的客户端
 * @param expire 被解析的expire参数
 * @param flags 代表命令的行为（EX/PX）
 * @param unit 给定的expire参数的原始时间单位（UNIT_SECONDS）
 * @param milliseconds 保存过期时间的计算结果
 * 
 * @retval C_OK "milliseconds" 参数将被设置为得到的绝对时间戳
 * @retval C_ERR 一个错误回复将被添加到给定客户端
 */
static int getExpireMillisecondsOrReply(client *c, robj *expire, int flags, int unit, long long *milliseconds) {
    // 获取过期时间所代表的绝对时间戳，将结果设置到"milliseconds"
    int ret = getLongLongFromObjectOrReply(c, expire, milliseconds, NULL);
    if (ret != C_OK) {
        return ret;
    }

    // 对于负数或者超出了长度上限，返回错误
    if (*milliseconds <= 0 || (unit == UNIT_SECONDS && *milliseconds > LLONG_MAX / 1000)) {
        /* Negative value provided or multiplication is gonna overflow. */
        addReplyErrorExpireTime(c);
        return C_ERR;
    }

    // 对于秒单位，转变为毫秒需要*1000
    if (unit == UNIT_SECONDS) *milliseconds *= 1000;

    // 对于PX/EX的过期类型，追加命令的开始时间
    if ((flags & OBJ_PX) || (flags & OBJ_EX)) {
        *milliseconds += commandTimeSnapshot();
    }

    // 对于负数，此处是因为数值越界了，返回错误
    if (*milliseconds <= 0) {
        /* Overflow detected. */
        addReplyErrorExpireTime(c);
        return C_ERR;
    }

    return C_OK;
}

#define COMMAND_GET 0
#define COMMAND_SET 1

/**
 * 该函数用于对在SET和GET命令中扩展的字符串参数执行通用校验。
 * 
 * Get特定命令 - PERSIST/DEL
 * Set特定命令 - XX/NX/GET
 * 通用命令 - EX/EXAT/PX/PXAT/KEEPTTL
 * 
 * 函数采用指向客户端、标志位、时间单位、指向过期对象的指针(如果需要确定)和
 * 命令类型（可以是COMMAND_GET或COMMAND_SET）。
 * 
 * 如果有任何的语法错误将返回C_ERR，否则返回C_OK。
 * 
 * 
 * 一旦参数被解析，将更新输入标识符。如果是EX/EXAT/PX/PXAT参数，则更新Unit和expire。
 * 如果设置了PX/PXAT，则Unit被更新为millisecond。
 * 
 * @param c 携带命令的客户端
 * @param flags 命令选项标识，有 EX/EXAT/PX/PXAT/PERSIST
 * @param unit 过期时间单位，有seconds/milliseconds
 * @param command_type 命令类型，有GET/SET
 * 
 * @retval C_OK 语法正确，解析成功
 * @retval C_ERR 语法错误
 */
int parseExtendedStringArgumentsOrReply(client *c, int *flags, int *unit, robj **expire, int command_type) {

    int j = command_type == COMMAND_GET ? 2 : 3;
    for (; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((opt[0] == 'n' || opt[0] == 'N') &&
            (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
            !(*flags & OBJ_SET_XX) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_NX;
        } else if ((opt[0] == 'x' || opt[0] == 'X') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_XX;
        } else if ((opt[0] == 'g' || opt[0] == 'G') &&
                   (opt[1] == 'e' || opt[1] == 'E') &&
                   (opt[2] == 't' || opt[2] == 'T') && opt[3] == '\0' &&
                   (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_GET;
        } else if (!strcasecmp(opt, "KEEPTTL") && !(*flags & OBJ_PERSIST) &&
            !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
            !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_KEEPTTL;
        } else if (!strcasecmp(opt,"PERSIST") && (command_type == COMMAND_GET) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) &&
               !(*flags & OBJ_KEEPTTL))
        {
            *flags |= OBJ_PERSIST;
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EXAT) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_EX;
            *expire = next;
            j++;
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_PX;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_EXAT;
            *expire = next;
            j++;
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PX) && next)
        {
            *flags |= OBJ_PXAT;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return C_ERR;
        }
    }
    return C_OK;
}

/**
 * SET key value [NX] [XX] [KEEPTTL] [GET] [EX <seconds>] [PX <milliseconds>]
 *      [EXAT <seconds-timestamp>][PXAT <milliseconds-timestamp>] 命令入口
 */
void setCommand(client *c) {
    robj *expire = NULL; // 默认无过期时间
    int unit = UNIT_SECONDS; // 默认过期时间单位为秒
    int flags = OBJ_NO_FLAGS; // 默认为无标识符

    // 解析SET的扩展参数是否合法
    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_SET) != C_OK) {
        return;
    }

    // 尝试对值进行编码压缩，以减少空间占用
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    // 
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

void setnxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_EX,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_PX,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

/**
 * 通用的GET命令
 * 
 * 如果键类型不匹配，则返回C_ERR
 * 
 * 如果键不存在或已过期，则返回C_OK
 * 如果键存在，则正常写入客户端输出缓冲区，返回C_OK
 */
int getGenericCommand(client *c) {
    robj *o; // 保存从db中对应key获取的值

    // 根据key获取条目，如果key不存在或已过期，则返回NULL，视作成功
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return C_OK;

    // 值的类型必须为String对象
    if (checkType(c,o,OBJ_STRING)) {
        return C_ERR;
    }

    // 将对象写入到客户端的输出缓冲区
    addReplyBulk(c,o);

    // 返回成功标识
    return C_OK;
}

/**
 * GET命令入口
 * 
 * 命令格式：GET key
 */
void getCommand(client *c) {
    getGenericCommand(c);
}


/**
 * GETEX命令入口
 * 
 * 命令格式：GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
 * 
 * 该命令扩展了GET命令，按照提供的额外选项设置Key的过期信息，它不是一个只读的命令，而不是存在写操作。
 * 如果未提供可选项，则其行为类似于GET命令。
 * 
 * 同一时间只能应用上述选项的一个：
 * 1.EX seconds: 设置过期TTL（以秒为单位）
 * 2.PX milliseconds: 设置过期TTL（以毫秒为单位）
 * 3.EXAT unix-time-seconds: 与EX类似，但是它不指定表示TTL的秒数，而是采用绝对Unix时间戳
 * 4.PXAT unix-time-milliseconds: 与PX类似，但是它不指定表示TTL的毫秒数，而是采用绝对Unix时间戳
 * 5.PERSIST 删除与key关联的TTL（Time to live）
 * 
 * @param c 携带命令的客户端
 * @retval 命令将返回批量字符串，错误或NULL
 */
void getexCommand(client *c) {
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    // 解析可选项，并分别设置到对应参数上，解析错误，直接返回
    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_GET) != C_OK) {
        return;
    }

    robj *o;
    // 从客户端对应的数据库中查询，如果不存在，直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return;

    // 对应值的类型是否为STRING，非STRING类型直接返回
    if (checkType(c,o,OBJ_STRING)) {
        return;
    }

    // 首先校验过期时间值
    long long milliseconds = 0;
    // 解析过期时间，并将其转换为决定的unix时间戳
    if (expire && getExpireMillisecondsOrReply(c, expire, flags, unit, &milliseconds) != C_OK) {
        return;
    }

    // 在对key过期或删除前必须这样做
    addReplyBulk(c,o);

    /**
     * 该命令永远不会按原样传播，它要么作为 PEXPIRE[AT], DEL, UNLINK 或 PERSIST 传播。
     * 这就是为什么它不需要在 feedAppendOnlyFile 中进行特殊处理以将相对时间转换为绝对过期时间。
     */
    if (((flags & OBJ_PXAT) || (flags & OBJ_EXAT)) && checkAlreadyExpired(milliseconds)) {
        // 当使用 PXAT/EXAT 指定了绝对时间戳，有可能时间戳晚于当前时间，因此在这种情况下需要删除key
        int deleted = dbGenericDelete(c->db, c->argv[1], server.lazyfree_lazy_expire, DB_FLAG_KEY_EXPIRED);
        serverAssert(deleted);
        robj *aux = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    } else if (expire) {
        // 处理设置了过期，但是尚未过期的情况
        setExpire(c,c->db,c->argv[1],milliseconds);
        // 如果存在 EX/PX/EXAT/PXAT 标识，且key尚未过期，则以 PXEXPIREATR 命令传播毫秒时间戳
        robj *milliseconds_obj = createStringObjectFromLongLong(milliseconds);
        rewriteClientCommandVector(c,3,shared.pexpireat,c->argv[1],milliseconds_obj);
        decrRefCount(milliseconds_obj);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",c->argv[1],c->db->id);
        server.dirty++;
    } else if (flags & OBJ_PERSIST) {
        // 移除键上的过期时间
        if (removeExpire(c->db, c->argv[1])) {
            signalModifiedKey(c, c->db, c->argv[1]);
            rewriteClientCommandVector(c, 2, shared.persist, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
            server.dirty++;
        }
    }
}

/**
 * GETDEL命令入口
 * 
 * 命令格式：GETDEL key
 * 
 * @param c 携带命令的客户端
 */
void getdelCommand(client *c) {
    // 当获取key对应的value报错时，直接返回
    if (getGenericCommand(c) == C_ERR) return;
    // 同步删除key
    if (dbSyncDelete(c->db, c->argv[1])) {
        // 作为 DEL 命令传播
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    }
}

/**
 * GETSET命令入口
 * 
 * 命令格式：GETSET key value
 * 
 * @param c 携带命令的客户端
 * 
 * 请注意，该命令从6.2.0已过期，推荐使用 SET GET 来替代。
 */
void getsetCommand(client *c) {
    // 当获取keu对应的value报错时，直接返回
    if (getGenericCommand(c) == C_ERR) return;
    // 尝试对value进行编码压缩
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    // 设置key
    setKey(c,c->db,c->argv[1],c->argv[2],0);
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    server.dirty++;

    // 作为 SET 命令传播
    rewriteClientCommandArgument(c,0,shared.set);
}

void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;

    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Return 0 when setting nothing on a non-existing string */
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset,sdslen(value)) != C_OK)
            return;

        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        /* Key exists, check type */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* Return existing string length when setting nothing */
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset,sdslen(value)) != C_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    if (sdslen(value) > 0) {
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        server.dirty++;
    }
    addReplyLongLong(c,sdslen(o->ptr));
}

/**
 * GETRANGE命令入口
 * 
 * 命令格式：GETRANGE key start end
 * 
 * @param c 携带命令的客户端
 */
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    // 获取开始长度
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
    // 获取结束长度
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;
    // 获取键对应的对象，如果键对象不存在，或类型不匹配，则直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL || checkType(c,o,OBJ_STRING)) 
        return;

    // 如果是INT编码
    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
        // 将数字类型转换为字符串
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        // 读取实际的值
        str = o->ptr;
        // 计算字符串长度
        strlen = sdslen(str);
    }

    // 如果start>end，则直接返回
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
    // 对于负数的索引，转换为正数
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    // 前提条件：end >= 0 && end < strlen，因此唯一无法返回任何内容的条件是 start > end
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk);
    } else {
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

/**
 * MGET命令入口
 * 
 * 命令格式：MGET key [key ...]
 */
void mgetCommand(client *c) {
    int j;

    // 添加要返回的数组长度，长度为所有参数-第一个的命令
    addReplyArrayLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        // 依次读取值
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            // 值为NULL，则写入NULL
            addReplyNull(c);
        } else {
            if (o->type != OBJ_STRING) {
                // 非字符串类型，则写入NULL
                addReplyNull(c);
            } else {
                // 写入对象
                addReplyBulk(c,o);
            }
        }
    }
}

/**
 * mset通用命令处理
 * 
 * @param c 携带命令的客户端
 * @param nx 1：不允许键存在，0：允许键存在
 */
void msetGenericCommand(client *c, int nx) {
    int j;

    // 如果命令的参数长度为偶数倍，则代表参数数量错误，应该为奇数
    if ((c->argc % 2) == 0) {
        addReplyErrorArity(c);
        return;
    }

    // 处理 NX 表示，当至少一个键存在时，MSETNX 语义是返回0，且不进行任何处理
    if (nx) {
        // j+=2 每隔两个处理一次
        for (j = 1; j < c->argc; j += 2) {
            // 检查可写的键是否存在，如果键已经存在，则立刻返回0
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                addReply(c, shared.czero);
                return;
            }
        }
    }

    int setkey_flags = nx ? SETKEY_DOESNT_EXIST : 0;
    for (j = 1; j < c->argc; j += 2) {
        // 尝试对值进行编码
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        // 写入键和值
        setKey(c, c->db, c->argv[j], c->argv[j + 1], setkey_flags);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
        /* In MSETNX, It could be that we're overriding the same key, we can't be sure it doesn't exist. */
        if (nx)
            setkey_flags = SETKEY_ADD_OR_UPDATE;
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

/**
 * MSET命令入口
 * 
 * 命令格式：MSET key value [key value ...]
 * 
 * @param c 携带命令的客户端
 */
void msetCommand(client *c) {
    // 允许键已存在
    msetGenericCommand(c,0);
}

/**
 * MSETNX命令入口
 * 
 * 命令格式：MSETNX key value [key value ...]
 */
void msetnxCommand(client *c) {
    // 不允许键已存在
    msetGenericCommand(c,1);
}

/**
 * 自增或自减命令
 * 
 * @param c 携带命令的客户端
 * @param incr 自增的数量
 */
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    // 查找键的值
    o = lookupKeyWrite(c->db,c->argv[1]);
    // 如果是STRING类型，则直接返回
    if (checkType(c,o,OBJ_STRING)) return;
    // 如果长度为0，即对象不存在，则直接返回
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) return;

    oldvalue = value;
    // 检查值是否越界
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    // 增加值
    value += incr;

    // 如果没有被其他所引用，则直接修改当前值
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        new = o;
        o->ptr = (void*)((long)value);
    } else { // 已被其他所引用，则在此基础上创建新的对象
        new = createStringObjectFromLongLongForValue(value);
        if (o) {
            // 原先值存在，则进行替换
            dbReplaceValue(c->db,c->argv[1],new);
        } else {
            // 原先值不存在，新添加
            dbAdd(c->db,c->argv[1],new);
        }
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c, value);
}

/**
 * INCR命令入口
 * 
 * 命令格式：INCR key
 * 
 * @param c 携带命令的客户端
 */
void incrCommand(client *c) {
    // 增加1
    incrDecrCommand(c,1);
}

/**
 * DECR命令入口
 * 
 * 命令格式：DECR
 * 
 * @param c 携带命令的客户端
 */
void decrCommand(client *c) {
    // 减去1
    incrDecrCommand(c,-1);
}

/**
 * INCRBY命令入口
 * 
 * 命令格式：INCRBY key decrement
 * 
 * @param c 携带命令的客户端
 */
void incrbyCommand(client *c) {
    long long incr;

    // 从参数increment中提取long long
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) 
        return;

    // 执行递增
    incrDecrCommand(c,incr);
}

/**
 * DECRBY命令入口
 * 
 * 命令格式：DECRBY key decrement
 * 
 * @param c 携带命令的客户端
 */
void decrbyCommand(client *c) {
    long long incr;

    // 从参数decrement中提取long long
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) 
        return;
    
    // 溢出检查：负 LLONG_MIN 将导致溢出
    if (incr == LLONG_MIN) {
        addReplyError(c, "decrement would overflow");
        return;
    }

    // 执行递减
    incrDecrCommand(c,-incr);
}

/**
 * INCRBYFLOAT命令入口
 * 
 * 命令格式：INCRBYFLOAT key increment
 * 
 * @param c 携带命令的客户端
 */
void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new;

    // 获取参数key对应的值
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 如果值类型不是OBJ_STRING，则直接返回
    if (checkType(c,o,OBJ_STRING)) 
        return;

    // 从对象上解析long double，并从参数increment中解析long double，只要有一个出错，则立刻返回
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    // 累加
    value += incr;
    // 如果值非法，则直接返回
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }
    // 创建新的对象
    new = createStringObjectFromLongDouble(value,1);
    if (o)
        // 对象已存在，则更新
        dbReplaceValue(c->db,c->argv[1],new);
    else
        // 对象不存在，则新增
        dbAdd(c->db,c->argv[1],new);

    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    server.dirty++;
    addReplyBulk(c,new);

    // 始终将 INCRBYFLOAT 作为具有最终值的 SET 命令进行复制，以确保浮点精度或格式的差异
    // 不会在副本中或AOF重启后产生差异
    rewriteClientCommandArgument(c,0,shared.set);
    rewriteClientCommandArgument(c,2,new);
    rewriteClientCommandArgument(c,3,shared.keepttl);
}

/**
 * APPEND命令入口
 * 
 * 命令格式：APPEND key value
 * 
 * @param c 携带命令的客户端
 */
void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    // 获取参数key对应的值
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        // 创建key
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        // key已存在，检查类型
        if (checkType(c,o,OBJ_STRING))
            return;

        // 读取append参数，其总是为sds
        append = c->argv[2];
        // 检查长度合法
        if (checkStringLength(c,stringObjectLen(o),sdslen(append->ptr)) != C_OK)
            return;

        // 追加值
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c,totlen);
}

/**
 * STRLEN命令入口
 * 
 * 命令格式：STRLEN key
 * 
 * @param c 携带命令的客户端
 */
void strlenCommand(client *c) {
    robj *o;
    // 读取key
    // 如果key不存在，或类型错误，直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,o,OBJ_STRING)) 
        return;
    // 返回长度
    addReplyLongLong(c,stringObjectLen(o));
}

/* LCS key1 key2 [LEN] [IDX] [MINMATCHLEN <len>] [WITHMATCHLEN] */
void lcsCommand(client *c) {
    uint32_t i, j;
    long long minmatchlen = 0;
    sds a = NULL, b = NULL;
    int getlen = 0, getidx = 0, withmatchlen = 0;
    robj *obja = NULL, *objb = NULL;

    obja = lookupKeyRead(c->db,c->argv[1]);
    objb = lookupKeyRead(c->db,c->argv[2]);
    if ((obja && obja->type != OBJ_STRING) ||
        (objb && objb->type != OBJ_STRING))
    {
        addReplyError(c,
            "The specified keys must contain string values");
        /* Don't cleanup the objects, we need to do that
         * only after calling getDecodedObject(). */
        obja = NULL;
        objb = NULL;
        goto cleanup;
    }
    obja = obja ? getDecodedObject(obja) : createStringObject("",0);
    objb = objb ? getDecodedObject(objb) : createStringObject("",0);
    a = obja->ptr;
    b = objb->ptr;

    for (j = 3; j < (uint32_t)c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1) - j;

        if (!strcasecmp(opt,"IDX")) {
            getidx = 1;
        } else if (!strcasecmp(opt,"LEN")) {
            getlen = 1;
        } else if (!strcasecmp(opt,"WITHMATCHLEN")) {
            withmatchlen = 1;
        } else if (!strcasecmp(opt,"MINMATCHLEN") && moreargs) {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&minmatchlen,NULL)
                != C_OK) goto cleanup;
            if (minmatchlen < 0) minmatchlen = 0;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* Complain if the user passed ambiguous parameters. */
    if (getlen && getidx) {
        addReplyError(c,
            "If you want both the length and indexes, please just use IDX.");
        goto cleanup;
    }

    /* Detect string truncation or later overflows. */
    if (sdslen(a) >= UINT32_MAX-1 || sdslen(b) >= UINT32_MAX-1) {
        addReplyError(c, "String too long for LCS");
        goto cleanup;
    }

    /* Compute the LCS using the vanilla dynamic programming technique of
     * building a table of LCS(x,y) substrings. */
    uint32_t alen = sdslen(a);
    uint32_t blen = sdslen(b);

    /* Setup an uint32_t array to store at LCS[i,j] the length of the
     * LCS A0..i-1, B0..j-1. Note that we have a linear array here, so
     * we index it as LCS[j+(blen+1)*i] */
    #define LCS(A,B) lcs[(B)+((A)*(blen+1))]

    /* Try to allocate the LCS table, and abort on overflow or insufficient memory. */
    unsigned long long lcssize = (unsigned long long)(alen+1)*(blen+1); /* Can't overflow due to the size limits above. */
    unsigned long long lcsalloc = lcssize * sizeof(uint32_t);
    uint32_t *lcs = NULL;
    if (lcsalloc < SIZE_MAX && lcsalloc / lcssize == sizeof(uint32_t)) {
        if (lcsalloc > (size_t)server.proto_max_bulk_len) {
            addReplyError(c, "Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len");
            goto cleanup;
        }
        lcs = ztrymalloc(lcsalloc);
    }
    if (!lcs) {
        addReplyError(c, "Insufficient memory, failed allocating transient memory for LCS");
        goto cleanup;
    }

    /* Start building the LCS table. */
    for (uint32_t i = 0; i <= alen; i++) {
        for (uint32_t j = 0; j <= blen; j++) {
            if (i == 0 || j == 0) {
                /* If one substring has length of zero, the
                 * LCS length is zero. */
                LCS(i,j) = 0;
            } else if (a[i-1] == b[j-1]) {
                /* The len LCS (and the LCS itself) of two
                 * sequences with the same final character, is the
                 * LCS of the two sequences without the last char
                 * plus that last char. */
                LCS(i,j) = LCS(i-1,j-1)+1;
            } else {
                /* If the last character is different, take the longest
                 * between the LCS of the first string and the second
                 * minus the last char, and the reverse. */
                uint32_t lcs1 = LCS(i-1,j);
                uint32_t lcs2 = LCS(i,j-1);
                LCS(i,j) = lcs1 > lcs2 ? lcs1 : lcs2;
            }
        }
    }

    /* Store the actual LCS string in "result" if needed. We create
     * it backward, but the length is already known, we store it into idx. */
    uint32_t idx = LCS(alen,blen);
    sds result = NULL;        /* Resulting LCS string. */
    void *arraylenptr = NULL; /* Deferred length of the array for IDX. */
    uint32_t arange_start = alen, /* alen signals that values are not set. */
             arange_end = 0,
             brange_start = 0,
             brange_end = 0;

    /* Do we need to compute the actual LCS string? Allocate it in that case. */
    int computelcs = getidx || !getlen;
    if (computelcs) result = sdsnewlen(SDS_NOINIT,idx);

    /* Start with a deferred array if we have to emit the ranges. */
    uint32_t arraylen = 0;  /* Number of ranges emitted in the array. */
    if (getidx) {
        addReplyMapLen(c,2);
        addReplyBulkCString(c,"matches");
        arraylenptr = addReplyDeferredLen(c);
    }

    i = alen, j = blen;
    while (computelcs && i > 0 && j > 0) {
        int emit_range = 0;
        if (a[i-1] == b[j-1]) {
            /* If there is a match, store the character and reduce
             * the indexes to look for a new match. */
            result[idx-1] = a[i-1];

            /* Track the current range. */
            if (arange_start == alen) {
                arange_start = i-1;
                arange_end = i-1;
                brange_start = j-1;
                brange_end = j-1;
            } else {
                /* Let's see if we can extend the range backward since
                 * it is contiguous. */
                if (arange_start == i && brange_start == j) {
                    arange_start--;
                    brange_start--;
                } else {
                    emit_range = 1;
                }
            }
            /* Emit the range if we matched with the first byte of
             * one of the two strings. We'll exit the loop ASAP. */
            if (arange_start == 0 || brange_start == 0) emit_range = 1;
            idx--; i--; j--;
        } else {
            /* Otherwise reduce i and j depending on the largest
             * LCS between, to understand what direction we need to go. */
            uint32_t lcs1 = LCS(i-1,j);
            uint32_t lcs2 = LCS(i,j-1);
            if (lcs1 > lcs2)
                i--;
            else
                j--;
            if (arange_start != alen) emit_range = 1;
        }

        /* Emit the current range if needed. */
        uint32_t match_len = arange_end - arange_start + 1;
        if (emit_range) {
            if (minmatchlen == 0 || match_len >= minmatchlen) {
                if (arraylenptr) {
                    addReplyArrayLen(c,2+withmatchlen);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,arange_start);
                    addReplyLongLong(c,arange_end);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,brange_start);
                    addReplyLongLong(c,brange_end);
                    if (withmatchlen) addReplyLongLong(c,match_len);
                    arraylen++;
                }
            }
            arange_start = alen; /* Restart at the next match. */
        }
    }

    /* Signal modified key, increment dirty, ... */

    /* Reply depending on the given options. */
    if (arraylenptr) {
        addReplyBulkCString(c,"len");
        addReplyLongLong(c,LCS(alen,blen));
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else if (getlen) {
        addReplyLongLong(c,LCS(alen,blen));
    } else {
        addReplyBulkSds(c,result);
        result = NULL;
    }

    /* Cleanup. */
    sdsfree(result);
    zfree(lcs);

cleanup:
    if (obja) decrRefCount(obja);
    if (objb) decrRefCount(objb);
    return;
}

