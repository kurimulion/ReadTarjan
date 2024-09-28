#include "CycleEnumeration.h"
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <ctime>
#include <chrono>
#include <queue>
#include <list>
#include <iostream>
#include <atomic>
#include <cilk/cilk_api.h>

using namespace std;

extern int timeWindow;

enum level
{
    node,
    edge,
    timestamp
};

struct dfs_out
{
    bool found;
    HashSetStack &blocked;
    BlockedSet &visited;
    Path *path;
    bool blk_flag;
    unsigned long visited_cnt;
};

/// DFS/BFS functions for pruning the search space

void dfsUtilPost(cont dfs_out out, Graph *g, int u, int start, HashSetStack &blocked, BlockedSet &visited,
                 bool blk_flag, Path *path, unsigned long stat_cnt)
{
    if (u != start && blk_flag)
    {
        blocked.insert(u);
        visited.remove(u);
    }

    blocked.include(visited);

    send_arguement(out, dfs_out{false, blocked, visited, path, blk_flag, stat_cnt});
}

// sequentially go through out-going edges of u
void dfsUtilIt(cont dfs_out out, Graph *g, int u, int start, int depth, HashSetStack &blocked, BlockedSet &visited,
               bool blk_flag, Path *path, unsigned long stat_cnt, int ind, bool found, int tstart = -1)
{
    if (found)
    {
        path->push_back(u);
        sned_argument(out, dfs{true, block, visited, path, false, stat_cnt});
    }
    else if (ind < g->offsArray[u + 1])
    {
        int w = g->edgeArray[ind].vertex;
        auto &tset = g->edgeArray[ind].tstamps;

        if (tstart != -1)
        {
            if (!edgeInTimeInterval(tstart, timeWindow, start, u, tset))
                cilk_spawn dfsUtilIt(out, g, u, start, depth, blocked, visited,
                                     false, path, stat_cnt, ind + 1, blk_flag, tstart);
        }
        else if (w == start)
        {
            path = new Path(depth + 2);
            path->push_back(w);
            path->push_back(u);
            sned_argument(out, dfs{true, block, visited, path, false, stat_cnt});
        }
        else if (visited.exists(w))
        {
            cilk_spawn dfsUtilIt(out, g, u, start, depth, blocked, visited,
                                 false, stat_cnt, ind + 1, false, tstart);
        }
        else if (((tstart != -1) || (tstart == -1) && (w > start)) &&
                 !blocked.exists(w) && !visited.exists(w))
        {
            cont dfs_out out_rec;
            cilk_spawn dfsUtil(out_rec, g, w, start, depth + 1, blocked, visited, stat_cnt, tstart);
            spawn_next dfsUtilIt(out, g, u, start, depth, out_rec.blocked, out_rec.visited,
                                 blk_flag && out_rec.blk_flag, out_rec.path, stat_cnt + out_rec.visited_cnt,
                                 ind + 1, out_rec.found, tstart);
        }
    }
    else
        cilk_spawn dfsUtilPost(out, g, u, start, blocked, visited, blk_flag, path, stat_cnt);
}

// Prune the search space and find the path using DFS
void dfsUtil(cont dfs_out out, Graph *g, int u, int start, int depth, HashSetStack &blocked, BlockedSet &visited,
             int tstart = -1)
{
    if (u == start && depth == 0)
    {
        Path *path = new Path(1);
        path->push_back(u);
        send_arguemnt(out, dfs_out{true, blocked, visited, false, path, 0});
    }
    else
    {
        visited.insert(u);
        cilk_spawn dfsUtilIt(out, g, u, start, depth, blocked, visited, true, NULL, 1, g->offsArray[u], false, tstart);
    }
}

// Different wrappers for the purpose of collecting statistics and easier profiling
// could be inlined
void findPath(cont dfs_out out, Graph *g, int u, int start, HashSetStack &blocked, int tstart)
{
    BlockedSet visited(g->getVertexNo());
    dfsUtil(out, g, u, start, 0, blocked, visited, tstart);
}

/// The main backtracking function

namespace
{
    struct ThreadDataGuard
    {
    public:
        ThreadDataGuard() : refCount(1)
        {
            blocked = new HashSetStack(true /*concurrent*/);
            current = new Cycle(true /*concurrent*/);
        }

        ThreadDataGuard(ThreadDataGuard *guard, int lvl, int pathSize) : refCount(1)
        {
            blocked = guard->blocked->clone(lvl);
            current = guard->current->clone(pathSize);
        }

        void incrementRefCount()
        {
            refCount++;
        }

        void decrementRefCount()
        {
            refCount--;
            if (refCount <= 0)
            {
                delete blocked;
                blocked = NULL;
                delete current;
                current = NULL;
                delete this;
            }
        }
        HashSetStack *blocked = NULL;
        Cycle *current = NULL;

        atomic<int> refCount;
    };

    void followPath(cont uint64_t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                    Path *current_path, bool branching, int tstart = -1, int level = 0);

    void RTCycleSubtask(cont uint64_t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked,
                        ThreadDataGuard *thrData, Path *current_path, int pathSize = 0, int ownderThread = -1,
                        int tstart = -1, int level = 0);

    void cyclesReadTarjanPost(cont uint_64t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                              pair<Path *, Path *> paths, Vector<Path *> allPaths, int level = 0, int tstart = -1)
    {
        cont uint64_t *res;
        res = malloc(sizeof(uint64_t) * allPaths.size());
        int thrId = __cilkrts_get_worker_number();
        int i = 0;
        for (auto current_path : allPaths)
        {
            // Forwarding the blocked set
            HashSetStack *new_blocked = blocked;
            thrData->incrementRefCount();
            cilk_spawn RTCycleSubtask(res[i], g, EdgeData(e.vertex, -1), current, new_blocked, thrData,
                                      current_path, pathSize, thrId, tstart, level);
            i++;
        }
        if (allPaths.size() == 0)
            thrData->decrementRefCount();
        // this is essentially a reduction PE
        spawn_next sum_arr(result, res, allPaths.size());
    }

    // collecting all paths from e, basically sequentially
    void cyclesReadTarjanIt(cont uint_64t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                            pair<Path *, Path *> paths, bool found, int ind, vector<Path *> &allPaths, Path *another_path,
                            int level = 0, int tstart = -1)
    {
        if (found)
            allPaths.push_back(another_path);

        if (ind < g->offsArray[e.vertex + 1])
        {
            int w = g->edgeArray[ind].vertex;

            auto &tset = g->edgeArray[ind].tstamps;

            Path *current_path = NULL;

            if (tstart != -1)
            {
                if (!edgeInTimeInterval(tstart, timeWindow, current->front(), e.vertex, tset))
                    cilk_spawn cyclesReadTarjanIt(result, g, e, current, blocked, thrData, paths,
                                                  false, ind + 1, allPaths, current_path, level, tstart);
            }
            else if ((tstart == -1) && (w < current->front()))
                cilk_spawn cyclesReadTarjanIt(result, g, e, current, blocked, thrData, paths,
                                              false, ind + 1, allPaths, current_path, level, tstart);
            else if (blocked->exists(w))
                cilk_spawn cyclesReadTarjanIt(result, g, e, current, blocked, thrData, paths,
                                              false, ind + 1, allPaths, current_path, level, tstart);
            else if (paths.first && w == paths.first->back())
            {
                current_path = paths.first;
                paths.first = NULL;
                cilk_spawn cyclesReadTarjanIt(result, g, e, current, blocked, thrData, paths,
                                              true, ind + 1, allPaths, current_path, level, tstart);
            }
            else if (paths.second && w == paths.second->back())
            {
                current_path = paths.second;
                paths.second = NULL;
                cilk_spawn cyclesReadTarjanIt(result, g, e, current, blocked, thrData, paths,
                                              true, ind + 1, allPaths, current_path, level, tstart);
            }
            else
            {
                cont dfs_out res;
                cilk_spawn findPath(res, w, current->front(), *blocked, tstart);
                spawn_next cyclesReadTarjanIt(result, g, e, current, res.blocked, thrData, paths,
                                              res.found, ind + 1, allPaths, res.path, level, tstart);
            }
        }
        else
        {
            cilk_spawn cyclesReadTarjanPost(result, g, e, current, blocked, thrData, paths, allPaths, level, tstart)
        }
    }

    void cyclesReadTarjan(cont uint_64t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                          pair<Path *, Path *> paths, int level = 0, int tstart = -1)
    {
        int pathSize = current->size();
        vector<Path *> allPaths;
        // Copy on steal
        ThreadDataGuard *newb = new ThreadDataGuard(thrData, level, pathSize);

        // Decrement the ref. count of the previous blocked map
        thrData->decrementRefCount();

        thrData = newb;
        // Update the pointers
        blocked = thrData->blocked;
        current = thrData->current;

        cilk_spawn cyclesReadTarjanIt(result, g, e, current, blocked, thrData, paths, false,
                                      g->offsArray[e.vertex], allPaths, nullptr, level, tstart);
    }

    void followPathInt(cont uint64_t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                       Path *current_path, int tstart, int level, Path *another_path, bool branching, int prev_vertex)
    {
        if (branching)
        {
            if (current_path)
                delete current_path;
            current_path = NULL;
            cilk_spawn cyclesReadTarjan(result, g, EdgeData(prev_vertex, -1), current, blocked, thrData,
                                        make_pair(current_path, another_path), level + 1, tstart);
        }
        else
        {
            delete current_path;
            current_path = NULL;
            delete blocked;
            blocked = NULL;
            thrData->decrementRefCount();
            // placeholder value
            // the result should be bucketed based on cycle size
            send_argument(result, 1);
        }
    }

    void followPathIt(cont uint64_t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                      Path *current_path, int tstart, int level, Path *another_path, bool branching, int prev_vertex, int ind)
    {
        if (!branching)
        {
            if (ind < g->offsArray[prev_vertex + 1])
            {
                int u = g->edgeArray[ind].vertex;
                auto &tset = g->edgeArray[ind].tstamps;

                if (tstart != -1)
                {
                    if (!edgeInTimeInterval(tstart, timeWindow, current->front(), prev_vertex, tset))
                        cilk_spawn followPathIt(result, g, e, current, blocked, thrData, current_path,
                                                tstart, level, another_path, branching, prev_vertex, ind + 1);
                }
                else if (u != current_path->back() && ((tstart != -1) || (tstart == -1) && (u > current->front())) &&
                         !blocked->exists(u) && !branching)
                {

                    // the other path is blocked
                    // if there's another cycle, there's a branch
                    cont dfs_out res;
                    cilk_spawn findPath(res, g, u, current->front(), *blocked, tstart);

                    spawn_next followPathIt(result, g, e, current, res.blocked, thrData, current_path,
                                            tstart, level, res.path, res.found, prev_vertex, ind + 1);
                }
            }
            else
            {
                cilk_spawn followPath(resut, g, e, current, blocked, thrData, current_path, branching, tstart, level);
            }
        }
        else
        {
            delete another_path;
            another_path = NULL;
            cilk_spawn followPathInt(result, g, e, current, blocked, thrData, current_path, tstart,
                                     level, another_path, branching, prev_vertex);
        }
    }

    void followPath(cont uint64_t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                    Path *current_path, bool branching, int tstart, int level)
    {
        Path *another_path = NULL;

        int prev_vertex = -1;
        // Add vertices from the found path to the current path until a branching is found
        if (current_path->back() != current->front())
        {
            prev_vertex = current_path->back();
            current_path->pop_back();
            current->push_back(prev_vertex);
            blocked->insert(prev_vertex);
            cilk_spawn followPathIt(result, g, e, current, blocked, thrData, current_path, tstart,
                                    level, another_path, false, prev_vertex, g->offsArray[prev_vertex]);
        }
        else
        {
            cilk_spawn followPathInt(result, g, e, current, blocked, thrData, current_path, tstart,
                                     level, another_path, branching, prev_vertex);
        }
    }

    void RTCycleSubtask(cont uint_64t result, Graph *g, EdgeData e, Cycle *current, HashSetStack *blocked,
                        ThreadDataGuard *thrData, Path *current_path,
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
            blocked = thrData->blocked;
            current = thrData->current;
        }
        else
        {
            blocked->setLevel(level);
            current->pop_back_until(pathSize);
        }

        blocked->incrementLevel();

        // not sure if it's just e or EdgeData(e.vertex, -1)
        cilk_spawn followPath(result, g, EdgeData(e.vertex, -1), current, blocked, thrData, result, current_path, false, tstart, level);
    }
}
/// ************ coarse-grained Read-Tarjan algorithm with time window - top level ************

void findAndFollow(cont uint64_t result, Graph *g, int node, Cycle *current, HashSetStack *blocked, ThreadDataGuard *thrData,
                   Path *current_path, int tstart, int level, bool found)
{
    if (found)
    {
        blocked->incrementLevel();
        cilk_spawn followPath(result, g, EdgeData(node, -1), cycle, blocked, thrData, current_path, false, tstart, 0);
    }
    else
    {
        send_arguement(result, 0);
    }
}

void findAndFollowPre(cont uint64_t result, Graph *g, int node, int ind, int j)
{
    int w = g->edgeArray[ind].vertex;
    auto &tset = g->edgeArray[ind].tstamps;
    int ts = tset[j];

    ThreadDataGuard *thrData = nullptr;
    thrData = new ThreadDataGuard();

    HashSetStack *blocked = thrData->blocked;

    Cycle *cycle = thrData->current;
    cycle->push_back(node);

    cont dfs_out res;
    cilk_spawn findPath(res, g, w, cycle->front(), *blocked, ts);
    spwan_next findAndFollow(result, g, node, cycle, res.blocked, thrData, res.path, ts, 0, res.found);
}

void spawnTasks(cont uint64_t result, Graph *g, int i, int ind, size_t min, size_t max, level l)
{
    if (min == max)
        send_arguement(result, 0);
    else
    {
        switch (l)
        {
        case node:
            if ((max - min) == 1)
            {
                if ((g->numNeighbors(min) != 0) && (g->numInEdges(min) != 0))
                    cilk_spawn spawnTasks(result, g, min, ind, size_t(g->offsArray[min]), size_t(g->offsArray[min + 1]), edge);
            }
            else if ((max - min) == 2)
            {
                if ((g->numNeighbors(min) != 0) && (g->numInEdges(min) != 0) && (g->numNeighbors(max - 1) != 0) && (g->numInEdges(max - 1) != 0))
                {
                    cont uint64_t res1, res2;
                    cilk_spawn spawnTasks(res1, g, min, ind, size_t(g->offsArray[min]), size_t(g->offsArray[min + 1]), edge);
                    cilk_spawn spawnTasks(res2, g, max - 1, ind, size_t(g->offsArray[max - 1]), size_t(g->offsArray[max]), edge);
                    // essentially this sould be a spwan_next call
                    // and it simply sums the number of cycles found in sub-tasks
                    spawn_next sum(result, res1, res2);
                }
                else if ((g->numNeighbors(max - 1) != 0) && (g->numInEdges(max - 1) != 0))
                {
                    cilk_spawn spawnTasks(result, g, min, ind, size_t(g->offsArray[min]), size_t(g->offsArray[min + 1]), edge);
                }
                else if ((g->numNeighbors(max - 1) != 0) && (g->numInEdges(max - 1) != 0))
                {
                    cilk_spawn spawnTasks(result, g, max - 1, ind, size_t(g->offsArray[max - 1]), size_t(g->offsArray[max]), edge);
                }
            }
            else
            {
                cont uint64_t res1, res2;
                int mid = (max + min) / 2;
                cilk_spawn spawnTasks(res1, g, i, ind, min, mid, l);
                cilk_spawn spawnTasks(res2, g, i, ind, mid, max, l);
                spwan_next sum(result, res1, res2);
            }
            break;
        case edge:
            if ((max - min) == 1)
                cilk_spawn spawnTasks(result, g, i, min, 0, g->edgeArray[min].tstamps.size(), timestamp);
            else if ((max - min) == 2)
            {
                cont uint64_t res1, res2;
                cilk_spawn spawnTasks(res1, g, i, min, 0, g->edgeArray[min].tstamps.size(), timestamp);
                cilk_spawn spawnTasks(res2, g, i, max - 1, 0, g->edgeArray[max - 1].tstamps.size(), timestamp);
                spawn_next sum(result, res1, res2);
            }
            else
            {
                cont uint64_t res1, res2;
                int mid = (max + min) / 2;
                cilk_spawn spawnTasks(res1, g, i, ind, min, mid, l);
                cilk_spawn spawnTasks(res2, g, i, ind, mid, max, l);
                spawn_next sum(result, res1, res2);
            }
            break;
        case timestamp:
            if ((max - min == 1))
                cilk_spawn findAndFollowPre(result, g, i, ind, min);
            else if ((max - min) == 2)
            {
                cont uint64_t res1, res2;
                cilk_spawn findAndFollowPre(res1, g, i, ind, min);
                cilk_spawn findAndFollowPre(res2, g, i, ind, max - 1);
                spawn_next sum(result, res1, res2)
            }
            else
            {
                cont uint64_t res1, res2;
                int mid = (max + min) / 2;
                cilk_spawn spawnTasks(res1, g, i, ind, min, mid, l);
                cilk_spawn spawnTasks(res2, g, i, ind, mid, max, l);
                spawn_next sum(result, res1, res2);
            }
        }
    }
}

// inline-able
void allCyclesReadTarjanCoarseGrainedTW(cont uint64_t result, Graph *g)
{
    spawnTasks(result, g, 0, 0, 0, size_t(g->getVertexNo()), node);
}
