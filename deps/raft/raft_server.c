/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* for varags */
#include <stdarg.h>

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

//记录运行时log
static void __log(raft_server_t *me_, raft_node_t* node, const char *fmt, ...)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    char buf[1024];
    va_list args;

    va_start(args, fmt);
    vsprintf(buf, fmt, args);

    if (me->cb.log)
        me->cb.log(me_, node, me->udata, buf);
}

/**
 * 初始化 raft server对象
 * 默认情况下 请求超时时间为200毫秒，选举超时时间位1000毫秒
 */
raft_server_t* raft_new()
{
    //申请内存
    raft_server_private_t* me =
        (raft_server_private_t*)calloc(1, sizeof(raft_server_private_t));

    if (!me){
        return NULL;
    }
    me->current_term = 0;//当前任期号
    me->voted_for = -1;//当前获得选票的候选人id
    me->timeout_elapsed = 0;//超时时间，从上一次获得心跳包到现在的时间
    me->request_timeout = 200;//请求超时时间200毫秒
    me->election_timeout = 1000; //选举超时时间1秒
    me->log = log_new();//log存取对象
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);//设置当前对象位跟随者
    me->current_leader = NULL;//当前领导这id为NULL
    return (raft_server_t*)me;
}

/**
 * 设置raft server 的回调函数
 */
void raft_set_callbacks(raft_server_t* me_, raft_cbs_t* funcs, void* udata)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;//强转对象类型

    memcpy(&me->cb, funcs, sizeof(raft_cbs_t));//拷贝回调函数的内存
    me->udata = udata;//server对象所需要使用的外部数据或者对象
    log_set_callbacks(me->log, &me->cb, me_);//设置log callbacks
}

/**
 * 释放raft server 对象
 */
void raft_free(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;//强转对象类型

    log_free(me->log);//释放日志对象申请的内存
    free(me_);//释放raft server申请的内存
}

/**
 * 清理raft server
 */
void raft_clear(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->current_term = 0;
    me->voted_for = -1;
    me->timeout_elapsed = 0;
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->current_leader = NULL;
    me->commit_idx = 0;
    me->last_applied_idx = 0;
    me->num_nodes = 0;
    me->node = NULL;
    me->voting_cfg_change_log_idx = 0;
    log_clear(me->log);
}

/**
 * 删除指定索引的日志
 */
void raft_delete_entry_from_idx(raft_server_t* me_, int idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (idx <= me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    log_delete(me->log, idx);
}

/**
 * 开始选举
 */
void raft_election_start(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    //记录开始选举的日志
    __log(me_, NULL, "election starting: %d %d, term: %d ci: %d",
          me->election_timeout, me->timeout_elapsed, me->current_term,
          raft_get_current_idx(me_));
    //改变自己身份，成为候选者，进行候选者应该做的工作
    raft_become_candidate(me_);
}
/**
 * 成为领导
 */
void raft_become_leader(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;
    __log(me_, NULL, "becoming leader term:%d", raft_get_current_term(me_));
    //成为领导人
    raft_set_state(me_, RAFT_STATE_LEADER);
        //对除了自己的节点发送成为领导人的消息
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i])
            continue;

        raft_node_t* node = me->nodes[i];//获取指定节点
        raft_node_set_next_idx(node, raft_get_current_idx(me_) + 1);//设置下一次需要发送给该节点的日志索引值
        raft_node_set_match_idx(node, 0);//设置已经复制给该节点的日志索引值 选举完清零
        raft_send_appendentries(me_, node);//添加条目到该节点
    }
}

/**
 * 成为候选者，开始选举
 */
void raft_become_candidate(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    __log(me_, NULL, "becoming candidate");

    raft_set_current_term(me_, raft_get_current_term(me_) + 1);
    for (i = 0; i < me->num_nodes; i++)
        raft_node_vote_for_me(me->nodes[i], 0);
    raft_vote(me_, me->node);
    me->current_leader = NULL;
    raft_set_state(me_, RAFT_STATE_CANDIDATE);

    /* We need a random factor here to prevent simultaneous candidates.
     * If the randomness is always positive it's possible that a fast node
     * would deadlock the cluster by always gaining a headstart. To prevent
     * this, we allow a negative randomness as a potential handicap. */
    me->timeout_elapsed = me->election_timeout - 2 * (rand() % me->election_timeout);

    for (i = 0; i < me->num_nodes; i++)
        if (me->node != me->nodes[i] && raft_node_is_voting(me->nodes[i]))
            raft_send_requestvote(me_, me->nodes[i]);
}

/**
 * 成为追随者
 */
void raft_become_follower(raft_server_t* me_)
{
    __log(me_, NULL, "becoming follower");
    raft_set_state(me_, RAFT_STATE_FOLLOWER);
}

/**
 * raft 的循环周期
 * msec_since_last_period 超时时间
 */
int raft_periodic(raft_server_t* me_, int msec_since_last_period)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->timeout_elapsed += msec_since_last_period;

    /* Only one voting node means it's safe for us to become the leader */
    //有跳票节点数量为1，当前节点有投票权限，当前节点不是leader，成为leader
    if (1 == raft_get_num_voting_nodes(me_) &&
        raft_node_is_voting(raft_get_my_node((void*)me)) &&
        !raft_is_leader(me_))
        raft_become_leader(me_);

    //
    if (me->state == RAFT_STATE_LEADER)
    {
        if (me->request_timeout <= me->timeout_elapsed)//如果已经到了心跳的时间
            raft_send_appendentries_all(me_);//对所有节点发送心跳包
    }
        /**如果选举超时时间小于心跳时间,默认情况选举超时时间一定大于心跳超时时间
         * 追随者没有收到一次没有收到心跳包 心跳超时时间累加 大于选举时间，开始成为候选人
         * 选举，当前节点必须是有跳票权限的节点
         **/
    else if (me->election_timeout <= me->timeout_elapsed)
    {
        if (1 < raft_get_num_voting_nodes(me_) &&
            raft_node_is_voting(raft_get_my_node(me_)))
            raft_election_start(me_);
    }

    if (me->last_applied_idx < me->commit_idx)
        return raft_apply_entry(me_);


    return 0;
}

/**
 * 获取指定索引的日志条目
 */
raft_entry_t* raft_get_entry_from_idx(raft_server_t* me_, int etyidx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_at_idx(me->log, etyidx);
}

int raft_voting_change_is_in_progress(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voting_cfg_change_log_idx != -1;
}

/**
 * 提交日志条目响应
 */
int raft_recv_appendentries_response(raft_server_t* me_,
                                     raft_node_t* node,
                                     msg_appendentries_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me_, node,
          "received appendentries response %s ci:%d rci:%d 1stidx:%d",
          r->success == 1 ? "SUCCESS" : "fail",
          raft_get_current_idx(me_),
          r->current_idx,
          r->first_idx);


    if (!node)
        return -1;

    /* Stale response -- ignore */
    if (r->current_idx != 0 && r->current_idx <= raft_node_get_match_idx(node))
        return 0;

    if (!raft_is_leader(me_))
        return -1;


    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (me->current_term < r->term)//如果当前服务的任期小于响应节点的任期号
    {
        raft_set_current_term(me_, r->term);//更新当前服务任期号
        raft_become_follower(me_);//当前服务成为跟随者
        return 0;
    }
    else if (me->current_term != r->term)//如果当前任期不等于响应任期，则忽略
        return 0;


    /* stop processing, this is a node we don't have in our configuration */
    if (!node)
        return 0;

    if (0 == r->success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        //assert(0 <= raft_node_get_next_idx(node));

        int next_idx = raft_node_get_next_idx(node);//获取这个节点下一个需要提交的日志索引
        assert(0 <= next_idx);//索引不能<=0
        if (r->current_idx < next_idx - 1)//该节点的发送日志索引值减一还比响应值大
        {
            //设置该节点下个需要发送的日志索引值等于响应索引值+1
            raft_node_set_next_idx(node, min(r->current_idx + 1, raft_get_current_idx(me_)));
        }
        else
        {
            //设置需发送的日志索引值
            raft_node_set_next_idx(node, next_idx - 1);
        }

        /* retry */
        //重新尝试发送该日志
        raft_send_appendentries(me_, node);
        return 0;
    }

    //响应的日志索引值<= 当前索引值 则出错
    assert(r->current_idx <= raft_get_current_idx(me_));

    raft_node_set_next_idx(node, r->current_idx + 1);
    //设置该节点所发送的日志最高索引值
    raft_node_set_match_idx(node, r->current_idx);


    if (!raft_node_is_voting(node) &&
        !raft_voting_change_is_in_progress(me_) &&
        raft_get_current_idx(me_) <= r->current_idx + 1 &&
        me->cb.node_has_sufficient_logs &&
        0 == raft_node_has_sufficient_logs(node)
        )
    {
        int e = me->cb.node_has_sufficient_logs(me_, me->udata, node);
        if (0 == e)
            raft_node_set_has_sufficient_logs(node);
    }
    //更新已提交idx
    /* Update commit idx */
    int votes = 1; /* include me */ //已提交此日志的选票
    int point = r->current_idx; //日志的当前索引值
    int i;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i] || !raft_node_is_voting(me->nodes[i]))
            continue;

        int match_idx = raft_node_get_match_idx(me->nodes[i]);//获取每个节点的已发送的最高日志索引值

        if (0 < match_idx)
        {
            raft_entry_t* ety = raft_get_entry_from_idx(me_, match_idx);
            if (ety->term == me->current_term && point <= match_idx)//该节点日志已提交，选票+1
                votes++;
        }
    }
    //保证大于一半的节点提交保持的此日志，这learder提交此日志进状态机
    if (raft_get_num_voting_nodes(me_) / 2 < votes && raft_get_commit_idx(me_) < point)
        raft_set_commit_idx(me_, point);

    /* Aggressively send remaining entries */
    //积极的发送剩余未提交的条目
    if (raft_get_entry_from_idx(me_, raft_node_get_next_idx(node)))
        raft_send_appendentries(me_, node);

    /* periodic applies committed entries lazily */

    return 0;
}
/**
 * 收到附加日志消息(跟随者)
 */
int raft_recv_appendentries(
    raft_server_t* me_,
    raft_node_t* node,
    msg_appendentries_t* ae,
    msg_appendentries_response_t *r
    )
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->timeout_elapsed = 0;//超时时间，从上一次获得心跳包到现在的时间 置0

    if (0 < ae->n_entries)//消息中的日志数目大于0
        __log(me_, node, "recvd appendentries t:%d ci:%d lc:%d pli:%d plt:%d #%d",
              ae->term,
              raft_get_current_idx(me_),
              ae->leader_commit,
              ae->prev_log_idx,
              ae->prev_log_term,
              ae->n_entries);

    r->term = me->current_term;//响应消息 任期号

    //如果服务当前状态是候选者 且任期号跟消息中的任期号一致
    if (raft_is_candidate(me_) && me->current_term == ae->term)
    {
        raft_become_follower(me_);
    }
    else if (me->current_term < ae->term)//任期小于消息任期
    {
        raft_set_current_term(me_, ae->term);
        r->term = ae->term;
        raft_become_follower(me_);
    }
    else if (ae->term < me->current_term)//消息任期比当前任期小 返回失败状态
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        __log(me_, node, "AE term %d is less than current term %d",
              ae->term, me->current_term);
        goto fail_with_current_idx;
    }

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    //我们收到的消息不是第一条消息
    if (0 < ae->prev_log_idx)
    {
        raft_entry_t* e = raft_get_entry_from_idx(me_, ae->prev_log_idx);//获得上一条消息

        //没获取到，失败
        if (!e)
        {
            __log(me_, node, "AE no log at prev_idx %d", ae->prev_log_idx);
            goto fail_with_current_idx;
        }

        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        if (raft_get_current_idx(me_) < ae->prev_log_idx)//如果节点当前索引小于上一个索引值 失败
            goto fail_with_current_idx;

        if (e->term != ae->prev_log_term)//上一条消息任期不等于指定上一条消息的任期
        {
            __log(me_, node, "AE term doesn't match prev_term (ie. %d vs %d) ci:%d pli:%d",
                  e->term, ae->prev_log_term, raft_get_current_idx(me_), ae->prev_log_idx);
            assert(me->commit_idx < ae->prev_log_idx);
            /* Delete all the following log entries because they don't match */
            raft_delete_entry_from_idx(me_, ae->prev_log_idx);
            r->current_idx = ae->prev_log_idx - 1;
            goto fail;
        }
    }

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    //强制跟随者的索引跟领导者一致，把不一致的多余日志删除
    if (ae->n_entries == 0 && 0 < ae->prev_log_idx && ae->prev_log_idx + 1 < raft_get_current_idx(me_))
    {
        assert(me->commit_idx < ae->prev_log_idx + 1);
        raft_delete_entry_from_idx(me_, ae->prev_log_idx + 1);
    }
    //当前索引等于上一个日志索引
    r->current_idx = ae->prev_log_idx;

    //判断消息中的日志在本地是否存在
    int i;
    for (i = 0; i < ae->n_entries; i++)
    {
        raft_entry_t* ety = &ae->entries[i];
        int ety_index = ae->prev_log_idx + 1 + i;
        raft_entry_t* existing_ety = raft_get_entry_from_idx(me_, ety_index);
        r->current_idx = ety_index;
        if (existing_ety && existing_ety->term != ety->term)
        {
            assert(me->commit_idx < ety_index);
            raft_delete_entry_from_idx(me_, ety_index);
            break;
        }
        else if (!existing_ety)
            break;
    }

    /* Pick up remainder in case of mismatch or missing entry */
    //追加消息中的日志
    for (; i < ae->n_entries; i++)
    {
        int e = raft_append_entry(me_, &ae->entries[i]);
        if (-1 == e)
            goto fail_with_current_idx;
        else if (RAFT_ERR_SHUTDOWN == e)
        {
            r->success = 0;
            r->first_idx = 0;
            return RAFT_ERR_SHUTDOWN;
        }
        r->current_idx = ae->prev_log_idx + 1 + i;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    //如果领导者的已提交状态机日志索引大于节点的日志索引
    if (raft_get_commit_idx(me_) < ae->leader_commit)
    {
        int last_log_idx = max(raft_get_current_idx(me_), 1);
        //提交日志
        raft_set_commit_idx(me_, min(last_log_idx, ae->leader_commit));
    }

    /* update current leader because we accepted appendentries from it */
    //记录当前领导者id
    me->current_leader = node;

    r->success = 1;
    r->first_idx = ae->prev_log_idx + 1;//发送的消息列表中第一条消息的索引值
    return 0;

fail_with_current_idx:
    r->current_idx = raft_get_current_idx(me_);
fail:
    r->success = 0;
    r->first_idx = 0;
    return -1;
}

/**
 * 有选票吗
 */
int raft_already_voted(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voted_for != -1;
}
/**
 * 是否同意投票
 */
static int __should_grant_vote(raft_server_private_t* me, msg_requestvote_t* vr)
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */

    if (!raft_node_is_voting(raft_get_my_node((void*)me)))
        return 0;

    if (vr->term < raft_get_current_term((void*)me))
        return 0;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    if (raft_already_voted((void*)me))
        return 0;

    /* Below we check if log is more up-to-date... */

    int current_idx = raft_get_current_idx((void*)me);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return 1;

    raft_entry_t* e = raft_get_entry_from_idx((void*)me, current_idx);
    if (e->term < vr->last_log_term)//最后一条日志任期值小于投票请求任期值，同意
        return 1;

    //消息任期值等于当前最后日志的任期值 且 索引小于或等于消息最后日志索引 同意
    if (vr->last_log_term == e->term && current_idx <= vr->last_log_idx)
        return 1;
    //不同意
    return 0;
}
/**
 * 收到投票请求(追随者)
 */
int raft_recv_requestvote(raft_server_t* me_,
                          raft_node_t* node,
                          msg_requestvote_t* vr,
                          msg_requestvote_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!node)
        node = raft_get_node(me_, vr->candidate_id);

    if (raft_get_current_term(me_) < vr->term)
    {
        raft_set_current_term(me_, vr->term);
        raft_become_follower(me_);
    }
    //是否同意投票
    if (__should_grant_vote(me, vr))
    {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        //节点当前状态不是领导 或者是候选者
        assert(!(raft_is_leader(me_) || raft_is_candidate(me_)));

        raft_vote_for_nodeid(me_, vr->candidate_id);
        r->vote_granted = 1;

        /* there must be in an election. */
        me->current_leader = NULL;

        me->timeout_elapsed = 0;//投票超时时间清零
    }
    else
    {
        /* It's possible the candidate node has been removed from the cluster but
         * hasn't received the appendentries that confirms the removal. Therefore
         * the node is partitioned and still thinks its part of the cluster. It
         * will eventually send a requestvote. This is error response tells the
         * node that it might be removed. */
        if (!node)
        {
            r->vote_granted = RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE;
            goto done;
        }
        else
            r->vote_granted = 0;
    }

done:
    __log(me_, node, "node requested vote: %d replying: %s",
          node,
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown");

    r->term = raft_get_current_term(me_);//当前任期号
    return 0;
}

/**
 * 统计投票是否占大多数
 */
int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

/**
 * 请求投票消息响应(候选者)
 */
int raft_recv_requestvote_response(raft_server_t* me_,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me_, node, "node responded to requestvote status: %s",
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown");

    if (!raft_is_candidate(me_))
    {
        return 0;
    }
    else if (raft_get_current_term(me_) < r->term)
    {
        raft_set_current_term(me_, r->term);//更新任期号
        raft_become_follower(me_);//成为追随者
        return 0;
    }
    else if (raft_get_current_term(me_) != r->term)//消息任期不等于当前任期 忽略此消息
    {
        /* The node who voted for us would have obtained our term.
         * Therefore this is an old message we should ignore.
         * This happens if the network is pretty choppy. */
        return 0;
    }
    __log(me_, node, "node responded to requestvote status:%s ct:%d rt:%d",
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown",
          me->current_term,
          r->term);

    switch (r->vote_granted)
    {
        case RAFT_REQUESTVOTE_ERR_GRANTED:
            if (node)
                raft_node_vote_for_me(node, 1);
            int votes = raft_get_nvotes_for_me(me_);
            if (raft_votes_is_majority(raft_get_num_voting_nodes(me_), votes))
                raft_become_leader(me_);
            break;

        case RAFT_REQUESTVOTE_ERR_NOT_GRANTED:
            break;

        case RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE:
            if (raft_node_is_voting(raft_get_my_node(me_)) &&
                me->connected == RAFT_NODE_STATUS_DISCONNECTING)
                return RAFT_ERR_SHUTDOWN;
            break;

        default:
            assert(0);

    }

    return 0;
}

/**
 * 客户端请求消息
 */
int raft_recv_entry(raft_server_t* me_,
                    msg_entry_t* e,
                    msg_entry_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    /* Only one voting cfg change at a time */
    if (raft_entry_is_voting_cfg_change(e))
        if (raft_voting_change_is_in_progress(me_))
            return RAFT_ERR_ONE_VOTING_CHANGE_ONLY;

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    __log(me_, NULL, "received entry t:%d id: %d idx: %d",
          me->current_term, e->id, raft_get_current_idx(me_) + 1);

    raft_entry_t ety;
    ety.term = me->current_term;
    ety.id = e->id;
    ety.type = e->type;

    memcpy(&ety.data, &e->data, sizeof(raft_entry_data_t));
    raft_append_entry(me_, &ety);//追加消息
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i] || !me->nodes[i] ||
            !raft_node_is_voting(me->nodes[i]))
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        int next_idx = raft_node_get_next_idx(me->nodes[i]);
        if (next_idx == raft_get_current_idx(me_))
            raft_send_appendentries(me_, me->nodes[i]);
    }

    /* if we're the only node, we can consider the entry committed */
    if (1 == raft_get_num_voting_nodes(me_))
        raft_set_commit_idx(me_, raft_get_current_idx(me_));

    r->id = e->id;
    r->idx = raft_get_current_idx(me_);
    r->term = me->current_term;

    if (raft_entry_is_voting_cfg_change(e))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return 0;
}

/**
 * 请求投票
 */
int raft_send_requestvote(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    msg_requestvote_t rv;//请求投票消息


    assert(node);
    assert(node != me->node);

    __log(me_, node, "sending requestvote to: %d", node);//记录发起了投票

    if (me->cb.send_requestvote)//如果存在请求投票回调函数，这执行请求投票回调函数
        me->cb.send_requestvote(me_, me->udata, node, &rv);
    return 0;
}

/**
 * 添加日志到当前raft server的日志条目中
 */
int raft_append_entry(raft_server_t* me_, raft_entry_t* ety)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return log_append_entry(me->log, ety);
}

/**
 * 把日志应用到状态机
 */
int raft_apply_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* Don't apply after the commit_idx */
    //最后应用到状态机的id 不能等于最后提交id
    if (me->last_applied_idx == me->commit_idx)
        return -1;

    int log_idx = me->last_applied_idx + 1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, log_idx);
    if (!ety)
        return -1;

    __log(me_, NULL, "applying log: %d, id: %d size: %d",
          me->last_applied_idx, ety->id, ety->data.len);

    me->last_applied_idx++;

    if (me->cb.applylog)
    {
        int e = me->cb.applylog(me_, me->udata, ety, me->last_applied_idx - 1);
        if (RAFT_ERR_SHUTDOWN == e)
            return RAFT_ERR_SHUTDOWN;
    }

    /* Membership Change: confirm connection with cluster */
    if (RAFT_LOGTYPE_ADD_NODE == ety->type)
    {
        int node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, log_idx);
        raft_node_set_has_sufficient_logs(raft_get_node(me_, node_id));
        if (node_id == raft_get_nodeid(me_))
            me->connected = RAFT_NODE_STATUS_CONNECTED;
    }

    /* voting cfg change is now complete */
    if (log_idx == me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    return 0;
}

raft_entry_t* raft_get_entries_from_idx(raft_server_t* me_, int idx, int* n_etys)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_from_idx(me->log, idx, n_etys);
}

int raft_send_appendentries(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(node);
    assert(node != me->node);

    if (!(me->cb.send_appendentries))
        return -1;

    msg_appendentries_t ae = {};
    ae.term = me->current_term; //任期号为当前任期号
    ae.leader_commit = raft_get_commit_idx(me_);//领导人已经提交的日志的索引值
    ae.prev_log_idx = 0;//新的日志条目紧随之前的索引值 0表示这是这个节点成为领导人后的第一个消息
    ae.prev_log_term = 0;//这条消息的上个上的的任期号
    ae.n_entries = 0; //日志条目数量
    ae.entries = NULL;//空消息 心跳包
    //获取此节点下次需要发送的日志条目索引值
    int next_idx = raft_node_get_next_idx(node);

    ae.entries = raft_get_entries_from_idx(me_, next_idx, &ae.n_entries);

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        raft_entry_t* prev_ety = raft_get_entry_from_idx(me_, next_idx - 1);
        ae.prev_log_idx = next_idx - 1;//上条消息的idx
        if (prev_ety)
            ae.prev_log_term = prev_ety->term;//上条消息的任期号
    }

    __log(me_, node, "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d",
          raft_get_current_idx(me_),
          raft_get_commit_idx(me_),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term);//记录发送日志

    me->cb.send_appendentries(me_, me->udata, node, &ae);

    return 0;
}

/**
 * 对所有节点发送一条消息
 */
void raft_send_appendentries_all(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    me->timeout_elapsed = 0;
    for (i = 0; i < me->num_nodes; i++)
        if (me->node != me->nodes[i])
            raft_send_appendentries(me_, me->nodes[i]);
}

raft_node_t* raft_add_node(raft_server_t* me_, void* udata, int id, int is_self)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* set to voting if node already exists */
    raft_node_t* node = raft_get_node(me_, id);
    if (node)
    {
        if (!raft_node_is_voting(node))
        {
            raft_node_set_voting(node, 1);
            return node;
        }
        else
            /* we shouldn't add a node twice */
            return NULL;
    }

    me->num_nodes++;
    me->nodes = (raft_node_t*)realloc(me->nodes, sizeof(raft_node_t*) * me->num_nodes);
    me->nodes[me->num_nodes - 1] = raft_node_new(udata, id);
    assert(me->nodes[me->num_nodes - 1]);
    if (is_self)
        me->node = me->nodes[me->num_nodes - 1];

    return me->nodes[me->num_nodes - 1];
}
raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, int id, int is_self)
{
    if (raft_get_node(me_, id))
        return NULL;

    raft_node_t* node = raft_add_node(me_, udata, id, is_self);
    if (!node)
        return NULL;

    raft_node_set_voting(node, 0);
    return node;
}

void raft_remove_node(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_node_t* new_array, *new_nodes;
    new_array = (raft_node_t*)calloc((me->num_nodes - 1), sizeof(raft_node_t*));
    new_nodes = new_array;

    int i, found = 0;
    for (i = 0; i<me->num_nodes; i++)
    {
        if (me->nodes[i] == node)
        {
            found = 1;
            continue;
        }
        *new_nodes = me->nodes[i];
        new_nodes++;
    }

    assert(found);

    me->num_nodes--;
    free(me->nodes);
    me->nodes = new_array;

    free(node);

}

/**
 * 获取已获得的选票数量
 */
int raft_get_nvotes_for_me(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, votes;

    for (i = 0, votes = 0; i < me->num_nodes; i++)
        if (me->node != me->nodes[i] && raft_node_is_voting(me->nodes[i]))
            if (raft_node_has_vote_for_me(me->nodes[i]))
                votes += 1;

    if (me->voted_for == raft_get_nodeid(me_))
        votes += 1;

    return votes;
}

void raft_vote(raft_server_t* me_, raft_node_t* node)
{
    raft_vote_for_nodeid(me_, node ? raft_node_get_id(node) : -1);
}

void raft_vote_for_nodeid(raft_server_t* me_, const int nodeid)

{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->voted_for = nodeid;
    if (me->cb.persist_vote)
        me->cb.persist_vote(me_, me->udata, nodeid);
}

int raft_msg_entry_response_committed(raft_server_t* me_,
                                      const msg_entry_response_t* r)
{
    raft_entry_t* ety = raft_get_entry_from_idx(me_, r->idx);
    if (!ety)
        return 0;

    /* entry from another leader has invalidated this entry message */
    if (r->term != ety->term)
        return -1;
    return r->idx <= raft_get_commit_idx(me_);
}

int raft_apply_all(raft_server_t* me_)
{
    while (raft_get_last_applied_idx(me_) < raft_get_commit_idx(me_))
    {
        int e = raft_apply_entry(me_);
        if (RAFT_ERR_SHUTDOWN == e)
            return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

int raft_entry_is_voting_cfg_change(raft_entry_t* ety)
{
    return RAFT_LOGTYPE_ADD_NODE == ety->type ||
           RAFT_LOGTYPE_DEMOTE_NODE == ety->type;
}

int raft_entry_is_cfg_change(raft_entry_t* ety)
{
    return (
        RAFT_LOGTYPE_ADD_NODE == ety->type ||
        RAFT_LOGTYPE_ADD_NONVOTING_NODE == ety->type ||
        RAFT_LOGTYPE_DEMOTE_NODE == ety->type ||
        RAFT_LOGTYPE_REMOVE_NODE == ety->type);
}

void raft_offer_log(raft_server_t* me_, raft_entry_t* ety, const int idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    int node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, idx);
    raft_node_t* node = raft_get_node(me_, node_id);
    int is_self = node_id == raft_get_nodeid(me_);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            if (!is_self)
            {
                raft_node_t* node = raft_add_non_voting_node(me_, NULL, node_id, is_self);
                assert(node);
            }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            node = raft_add_node(me_, NULL, node_id, is_self);
            assert(node);
            assert(raft_node_is_voting(node));
            break;

        case RAFT_LOGTYPE_DEMOTE_NODE:
            raft_node_set_voting(node, 0);
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node)
                raft_remove_node(me_, node);
            break;

        default:
            assert(0);
    }
}

void raft_pop_log(raft_server_t* me_, raft_entry_t* ety, const int idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    int node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, idx);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_DEMOTE_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_voting(node, 1);
            }
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            {
            int is_self = node_id == raft_get_nodeid(me_);
            raft_node_t* node = raft_add_non_voting_node(me_, NULL, node_id, is_self);
            assert(node);
            }
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            {
            int is_self = node_id == raft_get_nodeid(me_);
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_remove_node(me_, node);
            if (is_self)
                assert(0);
            }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_voting(node, 0);
            }
            break;

        default:
            assert(0);
            break;
    }
}
