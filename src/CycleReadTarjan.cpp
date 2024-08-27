#include "CycleEnumeration.h"
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <ctime>
#include <chrono>
#include <queue>
#include <list>
#include <iostream>
#include <cilk/cilk_api.h>

using namespace std;

extern int timeWindow;
extern ConcurrentCounter vertexVisits;

void ident_hist(void *v)
{
    new (v) CycleHist();
}

void combin_hist(void *l, void *r)
{
    CycleHist &h1 = *(CycleHist *)l;
    CycleHist h2 = *(CycleHist *)r;
    for (auto &pair : h2)
    {
        const int &cyc_size = pair.first;
        const unsigned long &cyc_num = pair.second;

        if (h1.find(cyc_size) == h1.end())
            h1[cyc_size] = 0;

        h1[cyc_size] += cyc_num;
    }
}

/// DFS/BFS functions for pruning the search space

// Prune the search space and find the path using DFS
bool dfsUtil(Graph *g, int u, int start, int depth, HashSetStack &blocked, BlockedSet &visited,
             bool &blk_flag, Path *&path, unsigned long &stat_cnt, int tstart = -1)
{

    blk_flag = false;
    bool this_blk_flag = true;

    if (u == start && depth == 0)
    {
        path = new Path(1);
        path->push_back(u);
        return true;
    }

    stat_cnt++;
    visited.insert(u);

    for (int ind = g->offsArray[u]; ind < g->offsArray[u + 1]; ind++)
    {
        int w = g->edgeArray[ind].vertex;
        auto &tset = g->edgeArray[ind].tstamps;

        if (tstart != -1)
        {
            if (!edgeInTimeInterval(tstart, timeWindow, start, u, tset))
                continue;
        }

        if (w == start)
        {
            path = new Path(depth + 2);
            path->push_back(w);
            path->push_back(u);
            return true;
        }
        else if (visited.exists(w))
        {
            this_blk_flag = false;
        }
        else if (((tstart != -1) || (tstart == -1) && (w > start)) &&
                 !blocked.exists(w) && !visited.exists(w))
        {
            bool rec_blk_flag = false;
            if (dfsUtil(g, w, start, depth + 1, blocked, visited, rec_blk_flag, path, stat_cnt, tstart))
            {
                path->push_back(u);
                return true;
            }
            this_blk_flag &= rec_blk_flag;
        }
    }

#if defined(BLK_FORWARD) && defined(SUCCESSFUL_DFS_BLK)
    if (u != start)
    {
        if (this_blk_flag)
        {
            blocked.insert(u);
            visited.remove(u);
        }
    }
#endif
    blk_flag = this_blk_flag;
    return false;
}

// Different wrappers for the purpose of collecting statistics and easier profiling
bool findPath(Graph *g, int u, int start, HashSetStack &blocked, Path *&path, int tstart)
{

    BlockedSet visited(g->getVertexNo());
    bool blck_flag = false;
    unsigned long vertex_visits = 0;
    bool found = dfsUtil(g, u, start, 0, blocked, visited, blck_flag, path, vertex_visits, tstart);

    vertexVisits += vertex_visits;

#if defined(BLK_FORWARD) && defined(SUCCESSFUL_DFS_BLK)
    if (u != start && blck_flag)
        blocked.insert(u);
#endif

#ifdef BLK_FORWARD
    if (!found)
        blocked.include(visited);
#endif
    return found;
}

bool dfsPrune(Graph *g, int u, int start, HashSetStack &blocked, Path *&path, int tstart)
{

    BlockedSet visited(g->getVertexNo());

    bool blck_flag = false;
    unsigned long vertex_visits = 0;
    bool found = dfsUtil(g, u, start, 0, blocked, visited, blck_flag, path, vertex_visits, tstart);

    vertexVisits += vertex_visits;

#if defined(BLK_FORWARD) && defined(SUCCESSFUL_DFS_BLK)
    if (u != start && blck_flag)
        blocked.insert(u);
#endif

    if (!found)
        blocked.include(visited);

    return found;
}

/// The main backtracking function

namespace
{
    struct ThreadDataGuard
    {
    public:
        ThreadDataGuard() : refCount(1), RefCountMutex()
        {
#ifdef BLK_FORWARD
            blocked = new HashSetStack(true /*concurrent*/);
#endif
            current = new Cycle(true /*concurrent*/);
        }

        ThreadDataGuard(ThreadDataGuard *guard, int lvl, int pathSize) : refCount(1), RefCountMutex()
        {
#ifdef BLK_FORWARD
            blocked = guard->blocked->clone(lvl);
#endif
            current = guard->current->clone(pathSize);
        }

        void incrementRefCount()
        {
            const std::lock_guard<std::mutex> lock(RefCountMutex);
            refCount++;
        }

        void decrementRefCount()
        {
            const lock_guard<std::mutex> lock(RefCountMutex);
            refCount--;
            if (refCount <= 0)
            {
#ifdef BLK_FORWARD
                delete blocked;
                blocked = NULL;
#endif
                delete current;
                current = NULL;
                delete this;
            }
        }

#ifdef BLK_FORWARD
        HashSetStack *blocked = NULL;
#endif
        Cycle *current = NULL;

        int refCount = 1;

        typedef std::mutex RefCountMutexType;
        RefCountMutexType RefCountMutex;
    };

    void followPath(Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                    CycleHist cilk_reducer(ident_hist, combin_hist) & result, Path *current_path, int tstart = -1, int level = 0);

    void RTCycleSubtask(Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked,
                        ThreadDataGuard *thrData, CycleHist cilk_reducer(ident_hist, combin_hist) & result, Path *current_path,
                        int pathSize = 0, int ownderThread = -1, int tstart = -1, int level = 0);

    void cyclesReadTarjan(Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                          CycleHist cilk_reducer(ident_hist, combin_hist) & result, pair<Path *, Path *> paths,
                          int level = 0, int tstart = -1)
    {
        int pathSize = current->size();
#ifdef NEIGHBOR_ORDER
        vector<Path *> allPaths;
#endif
        // Copy on steal
        ThreadDataGuard *newb = new ThreadDataGuard(thrData, level, pathSize);

        // Decrement the ref. count of the previous blocked map
        thrData->decrementRefCount();

        thrData = newb;
// Update the pointers
#ifdef BLK_FORWARD
        blocked = thrData->blocked;
#endif
        current = thrData->current;

        // exists a vertex w with a path to s*/
        for (int ind = g->offsArray[e.vertex]; ind < g->offsArray[e.vertex + 1]; ind++)
        {
            int w = g->edgeArray[ind].vertex;

            auto &tset = g->edgeArray[ind].tstamps;

            if (tstart != -1)
            {
                if (!edgeInTimeInterval(tstart, timeWindow, current->front(), e.vertex, tset))
                    continue;
            }

            if ((tstart == -1) && (w < current->front()))
                continue;
            if (blocked->exists(w))
                continue;

            Path *current_path = NULL;
            if (paths.first && w == paths.first->back())
            {
                current_path = paths.first;
                paths.first = NULL;
            }
            else if (paths.second && w == paths.second->back())
            {
                current_path = paths.second;
                paths.second = NULL;
            }
            else
            {
                bool found = false;
                found = findPath(g, w, current->front(), *blocked, current_path, tstart);
                if (!found)
                    continue;
            }

#ifdef NEIGHBOR_ORDER
            allPaths.push_back(current_path);
        }

        int thrId = __cilkrts_get_worker_number();
        for (auto current_path : allPaths)
        {
#endif
            // Forwarding the blocked set
#ifdef BLK_FORWARD
            HashSetStack *new_blocked = blocked;
            // new_blocked->incrementLevel();
#else
            HashSetStack *new_blocked = new HashSetStack(g->getVertexNo());
            for (auto c : *current)
                if (c != current->front())
                    new_blocked->insert(c);
#endif
            thrData->incrementRefCount();
            // cout << "outer level at " << e.vertex << " " << level << " inner level " << new_blocked->level() << std::endl;
            // cout << "level at " << e.vertex << " " << new_blocked->level() << std::endl;
            cilk_spawn RTCycleSubtask(g, EdgeData(e.vertex, -1), current, new_blocked, thrData,
                                      result, current_path, pathSize, thrId, tstart, level);
            // new_blocked->decrementLevel();
        }
        if (allPaths.size() == 0)
            thrData->decrementRefCount();
    }

    void followPath(Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                    CycleHist cilk_reducer(ident_hist, combin_hist) & result, Path *current_path, int tstart, int level)
    {
        Path *another_path = NULL;
        bool branching = false;

        int prev_vertex = -1;
        // Add vertices from the found path to the current path until a branching is found
        while (current_path->back() != current->front() && !branching)
        {
            prev_vertex = current_path->back();
            current_path->pop_back();
            current->push_back(prev_vertex);
            blocked->insert(prev_vertex);

            for (int ind = g->offsArray[prev_vertex]; ind < g->offsArray[prev_vertex + 1]; ind++)
            {
                int u = g->edgeArray[ind].vertex;
                auto &tset = g->edgeArray[ind].tstamps;

                if (tstart != -1)
                {
                    if (!edgeInTimeInterval(tstart, timeWindow, current->front(), prev_vertex, tset))
                        continue;
                }

                if (u != current_path->back() && ((tstart != -1) || (tstart == -1) && (u > current->front())) &&
                    !blocked->exists(u) && !branching)
                {

                    // the other path is blocked
                    // if there's another cycle, there's a branch
                    branching = dfsPrune(g, u, current->front(), *blocked, another_path, tstart);

                    if (branching)
                    {
#ifndef BLK_FORWARD
                        delete another_path;
                        another_path = NULL;
#endif
                        break;
                    }
                }
            }
        }

        if (branching)
        {
#ifndef BLK_FORWARD
            if (current_path)
                delete current_path;
            current_path = NULL;
#endif
            cyclesReadTarjan(g, EdgeData(prev_vertex, -1), current, blocked, thrData, result,
                             make_pair(current_path, another_path), level + 1, tstart);
        }
        else
        {
            delete current_path;
            current_path = NULL;
            recordCycle(current, result);

#if !defined(BLK_FORWARD)
            delete blocked;
            blocked = NULL;
#endif

            thrData->decrementRefCount();
        }

        // Remove all the vertices added after e.vertex
        // probably don't need this anymore because it's
        // corrected in RTCycleSubtask
        // while (1)
        // {
        //     int back = current->back();
        //     if (back == e.vertex)
        //         break;
        //     current->pop_back();
        // }
    }

    void RTCycleSubtask(Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked,
                        ThreadDataGuard *thrData, CycleHist cilk_reducer(ident_hist, combin_hist) & result, Path *current_path,
                        int pathSize, int ownderThread, int tstart, int level)
    {
        int thisThread = __cilkrts_get_worker_number();
        if (ownderThread != thisThread)
        {
            // Copy on steal
            ThreadDataGuard *newb = new ThreadDataGuard(thrData, level, pathSize);

            // Decrement the ref. count of the previous blocked map
            thrData->decrementRefCount();

            thrData = newb;
// Update the pointers
#ifdef BLK_FORWARD
            blocked = thrData->blocked;
#endif
            current = thrData->current;
        }
        else
        {
#ifdef BLK_FORWARD
            blocked->setLevel(level);
#endif
            current->pop_back_until(pathSize);
        }

        blocked->incrementLevel();

        // not sure if it's just e or EdgeData(e.vertex, -1)
        followPath(g, EdgeData(e.vertex, -1), current, blocked, thrData, result, current_path, tstart, level);

#ifndef BLK_FORWARD
        delete blocked;
        blocked = NULL;
#endif
    }
}
/// ************ coarse-grained Read-Tarjan algorithm with time window - top level ************

void allCyclesReadTarjanCoarseGrainedTW(Graph *g, CycleHist &result, int numThreads)
{
    CycleHist cilk_reducer(ident_hist, combin_hist) resultHistogram;

    cilk_for(size_t i = 0; i < size_t(g->getVertexNo()); i++)
    {
        if ((g->numNeighbors(i) != 0) && (g->numInEdges(i) != 0))
        {
            cilk_for(size_t ind = size_t(g->offsArray[i]); ind < size_t(g->offsArray[i + 1]); ind++)
            {
                int w = g->edgeArray[ind].vertex;
                auto &tset = g->edgeArray[ind].tstamps;

                cilk_for(size_t j = 0; j < size_t(tset.size()); j++)
                {
                    int ts = tset[j];

                    ThreadDataGuard *thrData = nullptr;
                    thrData = new ThreadDataGuard();

#if defined(BLK_FORWARD)
                    HashSetStack *blocked = thrData->blocked;
#else
                    HashSetStack *blocked = new HashSetStack(graph->getVertexNo());
#endif

                    Cycle *cycle = thrData->current;
                    cycle->push_back(i);

                    Path *current_path = NULL;
                    bool found = findPath(g, w, cycle->front(), *blocked, current_path, ts);
                    if (found)
                    {
#if defined(BLK_FORWARD)
                        blocked->incrementLevel();
#endif
                        cilk_spawn followPath(g, EdgeData(i, -1), cycle, blocked, thrData, resultHistogram, current_path, ts, 0);
                    }

                    // delete cycle;
                }
            }
        }
    }

    result = resultHistogram;
}
