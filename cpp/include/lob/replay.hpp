// replay.hpp: the replay state machine of replay.py in C++ (header-only), for either book container.
//
// Same rules as replay.py, per session (one WebSocket connection; every session starts without a book):
//   * snapshots are placed by their update id L, not by their receive time;
//   * sync: the first event with u > L must satisfy U <= L + 1, else the snapshot is unusable;
//   * synced: u <= last_u -> stale event (ignored); U > last_u + 1 -> gap (book dropped until a resync);
//   * every later snapshot is compared with the book in the state of update id L, over the snapshot's
//     n best levels per side; if L falls inside the next event, the levels of that event are excluded;
//   * a mismatch is "untracked" when the level is missing from the book and lies beyond the depth of
//     the sync snapshot; any other mismatch is "unexplained".
// replay.py is the reference: the tests and crosscheck.py check that both give identical results.
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lob/book.hpp"

namespace lob {

enum EventStatus : std::int8_t { kApplied = 0, kUnsynced = 1, kStale = 2 };

struct Mismatch {
    int side;
    Price price;
    Qty book_qty;   // 0 = level missing from the book
    Qty snap_qty;   // 0 = level missing from the snapshot
    bool untracked;
};

struct Check {
    std::int64_t snap_rx_ns = 0;
    std::int64_t last_update_id = 0;
    bool straddle = false;             // L was inside an event: that event's levels were excluded
    std::int64_t compared = 0;
    std::int64_t excluded = 0;
    std::vector<Mismatch> mismatches;  // bids by ascending price, then asks by ascending price (as replay.py)
    bool crossed = false;
    std::int64_t book_levels = 0;
};

struct Gap {
    std::int64_t rx_ns, expected_first_id, got_first_id;
};

struct Sync {
    std::int64_t snap_rx_ns, event_rx_ns, last_update_id;
};

struct SessionResult {
    std::int64_t session = 0, first_rx = 0, last_rx = 0;
    std::int64_t events = 0, applied = 0, stale = 0, unsynced_events = 0, removed_missing = 0;
    std::int64_t unusable_snaps = 0, skipped_snaps = 0, snaps_after_end = 0, unsynced_ns = 0;
    std::vector<Gap> gaps;
    std::vector<Sync> syncs;
    std::vector<Check> checks;
    // Depth of the last sync snapshot: the book is complete for bids >= bid_floor and asks <= ask_cap.
    // Empty when that side of the snapshot was empty (then every level of that side is "beyond").
    std::optional<Price> bid_floor, ask_cap;
};

// Power-of-two bucket of a distance: the number of bits needed to write x, i.e. 0 -> 0, 1 -> 1,
// 2..3 -> 2, 4..7 -> 3, ..., and bucket k holds 2^(k-1) <= x < 2^k. (C++20 calls it std::bit_width.)
// A plain loop: it only runs when profiling.
inline std::size_t pow2_bucket(std::uint64_t x) {
    std::size_t k = 0;
    for (; x != 0; x >>= 1) ++k;
    return k;
}

// Benchmark profile of the applied level updates (filled only when profiling is on).
// 'better' = number of levels with a better price than the updated one; for a sorted vector, an insert
// or erase at that price shifts exactly that many elements.
struct Profile {
    std::int64_t inserts = 0, erases = 0, in_place = 0, missing = 0;  // what each level update did
    std::int64_t shifted = 0;               // sum of 'better' over inserts and erases
    std::array<std::int64_t, 6> by_depth{};  // inserts + erases by 'better': 0, 1-9, 10-99, ..., >= 10000
    std::int64_t events = 0, bid_levels = 0, ask_levels = 0;  // book size summed over applied events
    // Distance in ticks from each applied update's price to the best price of its side, just before the
    // update, by pow2_bucket: 'behind' when the price is at or behind the best (distance 0 = the best
    // itself), 'ahead' when it is better than the best (a new best price); 'no_best': the side was empty.
    std::array<std::int64_t, 64> behind{}, ahead{};
    std::int64_t no_best = 0;
    // Updates that erase the best level itself: distance in ticks to the level that becomes the new best,
    // by pow2_bucket ('next_gap'), or 'last_level' when the side had no other level.
    std::array<std::int64_t, 64> next_gap{};
    std::int64_t last_level = 0;
};

template <class Book>
class Replayer {
public:
    explicit Replayer(std::size_t top_n, bool profile = false) : top_n_(top_n), profiling_(profile) {}

    // All snapshots, given once before the events, in receive order. Snapshot i owns the levels
    // [offsets[i], offsets[i + 1]) of the side / price / qty arrays.
    void set_snapshots(std::size_t n, const std::int64_t* rx, const std::int64_t* session,
                       const std::int64_t* last_id, const std::int64_t* offsets, const std::int8_t* side,
                       const Price* price, const Qty* qty) {
        if (started_) throw std::logic_error("set_snapshots must be called before the first event");
        snaps_.clear();
        snaps_.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            Snapshot s{rx[i], session[i], last_id[i], {}, {}};
            for (std::int64_t k = offsets[i]; k < offsets[i + 1]; ++k)
                (side[k] == kBid ? s.bids : s.asks).push_back(Level{price[k], qty[k]});
            std::sort(s.bids.begin(), s.bids.end(), [](const Level& a, const Level& b) { return a.price > b.price; });
            std::sort(s.asks.begin(), s.asks.end(), [](const Level& a, const Level& b) { return a.price < b.price; });
            snaps_.push_back(std::move(s));
        }
        // Order by session, then update id. stable_sort keeps snapshots with equal ids in receive order,
        // like the stable sort of replay.py.
        order_.resize(n);
        std::iota(order_.begin(), order_.end(), std::size_t{0});
        std::stable_sort(order_.begin(), order_.end(), [this](std::size_t a, std::size_t b) {
            const Snapshot& x = snaps_[a];
            const Snapshot& y = snaps_[b];
            return x.session != y.session ? x.session < y.session : x.last_update_id < y.last_update_id;
        });
    }

    // Process consecutive depth events (e.g. one hour at a time; the state carries over between calls).
    // Event i owns the levels [offsets[i], offsets[i + 1]). The output arrays (all optional, length n)
    // receive the status of each event and the top of book after it (0 when not synced or side empty).
    // ns, if given, receives the time spent in each event (steady_clock; the top-of-book output excluded).
    void process(std::size_t n, const std::int64_t* rx, const std::int64_t* session, const std::int64_t* first_id,
                 const std::int64_t* last_id, const std::int64_t* offsets, const std::int8_t* side,
                 const Price* price, const Qty* qty, std::int8_t* status, Price* bid_px, Qty* bid_qty,
                 Price* ask_px, Qty* ask_qty, std::int64_t* ns = nullptr) {
        using clock = std::chrono::steady_clock;
        started_ = true;
        for (std::size_t i = 0; i < n; ++i) {
            const std::int64_t b = offsets[i];
            const auto m = static_cast<std::size_t>(offsets[i + 1] - b);
            EventStatus st;
            if (ns) {
                const auto t0 = clock::now();
                st = on_event(rx[i], session[i], first_id[i], last_id[i], side + b, price + b, qty + b, m);
                ns[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - t0).count();
            } else {
                st = on_event(rx[i], session[i], first_id[i], last_id[i], side + b, price + b, qty + b, m);
            }
            if (status) status[i] = st;
            if (bid_px) {
                const std::optional<Level> bb = synced_ ? book_.bids.best() : std::nullopt;
                const std::optional<Level> ba = synced_ ? book_.asks.best() : std::nullopt;
                bid_px[i] = bb ? bb->price : 0;
                bid_qty[i] = bb ? bb->qty : 0;
                ask_px[i] = ba ? ba->price : 0;
                ask_qty[i] = ba ? ba->qty : 0;
            }
        }
    }

    // Close the current session (call once after the last event).
    void finish() {
        if (has_session_) finish_session();
    }

    const std::vector<SessionResult>& sessions() const { return results_; }
    const Profile& profile() const { return profile_; }

private:
    struct Snapshot {
        std::int64_t rx_ns, session, last_update_id;
        std::vector<Level> bids, asks;  // best first
    };

    EventStatus on_event(std::int64_t rx, std::int64_t sid, std::int64_t U, std::int64_t u, const std::int8_t* side,
                         const Price* price, const Qty* qty, std::size_t n_levels) {
        if (!has_session_ || sid != cur_.session) start_session(sid);
        SessionResult& s = cur_;
        ++s.events;
        if (s.events == 1) {
            s.first_rx = rx;
            unsynced_since_ = rx;
        }
        s.last_rx = rx;

        // 1. sequence check: a gap makes the book wrong from here on
        if (synced_ && U > book_.last_update_id + 1) {
            s.gaps.push_back(Gap{rx, book_.last_update_id + 1, U});
            synced_ = false;
            unsynced_since_ = rx;
        }
        // 2. snapshots whose state lies before the end of this event (L < u): sync or validate now
        while (snap_pos_ < snap_end_ && snaps_[order_[snap_pos_]].last_update_id < u) {
            use_snapshot(snaps_[order_[snap_pos_]], rx, U, side, price, n_levels);
            ++snap_pos_;
        }
        // 3. apply the event
        if (!synced_) {
            ++s.unsynced_events;
            return kUnsynced;
        }
        if (u <= book_.last_update_id) {
            ++s.stale;
            return kStale;
        }
        if (profiling_)
            s.removed_missing += apply_profiled(side, price, qty, n_levels, u);
        else
            s.removed_missing += book_.apply(side, price, qty, n_levels, u);
        ++s.applied;
        return kApplied;
    }

    // Same result as book_.apply(), level by level, recording where each update lands first.
    std::int64_t apply_profiled(const std::int8_t* side, const Price* price, const Qty* qty, std::size_t n,
                                std::int64_t u) {
        std::int64_t missing = 0;
        for (std::size_t k = 0; k < n; ++k) {
            const Side sd = static_cast<Side>(side[k]);
            const Probe pr = sd == kBid ? book_.bids.probe(price[k]) : book_.asks.probe(price[k]);
            const bool moves = qty[k] == 0 ? pr.found : !pr.found;  // an erase or an insert
            if (qty[k] == 0) (pr.found ? profile_.erases : profile_.missing) += 1;
            else (pr.found ? profile_.in_place : profile_.inserts) += 1;
            if (moves) {
                profile_.shifted += static_cast<std::int64_t>(pr.better);
                std::size_t bucket = 0;
                for (std::size_t x = pr.better; x > 0 && bucket < 5; x /= 10) ++bucket;
                ++profile_.by_depth[bucket];
            }
            const bool erase = qty[k] == 0 && pr.found;
            if (sd == kBid) profile_distance(book_.bids, kBid, price[k], erase);
            else profile_distance(book_.asks, kAsk, price[k], erase);
            if (!book_.set(sd, price[k], qty[k])) ++missing;
        }
        book_.last_update_id = u;
        ++profile_.events;
        profile_.bid_levels += static_cast<std::int64_t>(book_.bids.size());
        profile_.ask_levels += static_cast<std::int64_t>(book_.asks.size());
        return missing;
    }

    // Before an update at price p on side s: its distance in ticks from the best price of s and, if the
    // update erases the best level itself, the distance from that level to the next one.
    template <class SideT>
    void profile_distance(const SideT& s, Side sd, Price p, bool erase) {
        Price best = 0, next = 0;
        std::size_t seen = 0;  // levels visited: we only need the best two
        s.for_each_best_first([&](const Level& l) {
            (seen == 0 ? best : next) = l.price;
            return ++seen < 2;
        });
        if (seen == 0) {
            ++profile_.no_best;
            return;
        }
        const Price d = sd == kBid ? best - p : p - best;  // > 0: behind the best; < 0: better than the best
        if (d >= 0) ++profile_.behind[pow2_bucket(static_cast<std::uint64_t>(d))];
        else ++profile_.ahead[pow2_bucket(static_cast<std::uint64_t>(-d))];
        if (erase && d == 0) {  // the best level itself is removed
            if (seen < 2) ++profile_.last_level;
            else ++profile_.next_gap[pow2_bucket(static_cast<std::uint64_t>(sd == kBid ? best - next : next - best))];
        }
    }

    void use_snapshot(const Snapshot& sn, std::int64_t rx, std::int64_t U, const std::int8_t* side,
                      const Price* price, std::size_t n_levels) {
        SessionResult& s = cur_;
        const std::int64_t L = sn.last_update_id;
        if (!synced_) {
            if (L + 1 >= U) {  // U <= L + 1 <= u: the snapshot bridges into this event
                book_.load_snapshot(L, sn.bids, sn.asks);
                s.bid_floor = sn.bids.empty() ? std::nullopt : std::optional<Price>(sn.bids.back().price);
                s.ask_cap = sn.asks.empty() ? std::nullopt : std::optional<Price>(sn.asks.back().price);
                s.syncs.push_back(Sync{sn.rx_ns, rx, L});
                s.unsynced_ns += rx - *unsynced_since_;
                unsynced_since_.reset();
                synced_ = true;
            } else {
                ++s.unusable_snaps;  // older than the data we have
            }
            return;
        }
        const std::int64_t last = book_.last_update_id;
        if (L < last) {  // cannot happen with snapshots sorted by L; counted, as in replay.py
            ++s.skipped_snaps;
            return;
        }
        Check c;
        c.snap_rx_ns = sn.rx_ns;
        c.last_update_id = L;
        c.straddle = L > last;
        excl_bid_.clear();
        excl_ask_.clear();
        if (c.straddle)  // last < L < u: the snapshot holds part of this event's updates
            for (std::size_t k = 0; k < n_levels; ++k) (side[k] == kBid ? excl_bid_ : excl_ask_).push_back(price[k]);
        std::sort(excl_bid_.begin(), excl_bid_.end());
        std::sort(excl_ask_.begin(), excl_ask_.end());
        compare_side(book_.bids, kBid, sn.bids, excl_bid_, std::greater<Price>{}, c);
        compare_side(book_.asks, kAsk, sn.asks, excl_ask_, std::less<Price>{}, c);
        c.crossed = book_.crossed();
        c.book_levels = static_cast<std::int64_t>(book_.bids.size() + book_.asks.size());
        s.checks.push_back(std::move(c));
    }

    // Compare one side of the book with the snapshot's n best levels of that side (ref: best first).
    // better(a, b): price a is better than b on this side. Both lists are walked from the best price
    // down, like the merge step of merge sort, so every price present in either list is seen once.
    template <class SideT, class Better>
    void compare_side(const SideT& mine, int side, const std::vector<Level>& ref, const std::vector<Price>& excl,
                      Better better, Check& c) {
        if (ref.empty()) return;
        const std::size_t k = std::min(top_n_, ref.size());
        const Price edge = ref[k - 1].price;  // the n-th best snapshot price: the checked range ends here
        in_range_.clear();
        mine.for_each_best_first([&](const Level& l) {
            if (better(edge, l.price)) return false;  // beyond the edge: stop
            in_range_.push_back(l);
            return true;
        });
        found_.clear();
        std::size_t i = 0, j = 0;
        while (i < in_range_.size() || j < k) {
            Price p;
            Qty a = 0, b = 0;
            if (j == k || (i < in_range_.size() && better(in_range_[i].price, ref[j].price))) {
                p = in_range_[i].price;  // only in the book
                a = in_range_[i++].qty;
            } else if (i == in_range_.size() || better(ref[j].price, in_range_[i].price)) {
                p = ref[j].price;  // only in the snapshot
                b = ref[j++].qty;
            } else {
                p = in_range_[i].price;  // in both
                a = in_range_[i++].qty;
                b = ref[j++].qty;
            }
            if (std::binary_search(excl.begin(), excl.end(), p)) {
                ++c.excluded;
                continue;
            }
            ++c.compared;
            if (a != b) found_.push_back(Mismatch{side, p, a, b, a == 0 && beyond_sync_depth(side, p)});
        }
        if (side == kBid) std::reverse(found_.begin(), found_.end());  // ascending prices, as in replay.py
        c.mismatches.insert(c.mismatches.end(), found_.begin(), found_.end());
    }

    bool beyond_sync_depth(int side, Price p) const {
        if (side == kBid) return !cur_.bid_floor || p < *cur_.bid_floor;
        return !cur_.ask_cap || p > *cur_.ask_cap;
    }

    void start_session(std::int64_t sid) {
        if (has_session_) finish_session();
        if (!seen_.insert(sid).second)
            throw std::invalid_argument("session " + std::to_string(sid) + " appears again after another session started");
        cur_ = SessionResult{};
        cur_.session = sid;
        has_session_ = true;
        book_.clear();
        synced_ = false;
        unsynced_since_.reset();
        // this session's snapshots: a contiguous run of order_ (sorted by session, then update id)
        const auto lo = std::lower_bound(order_.begin(), order_.end(), sid,
                                         [this](std::size_t i, std::int64_t v) { return snaps_[i].session < v; });
        const auto hi = std::upper_bound(order_.begin(), order_.end(), sid,
                                         [this](std::int64_t v, std::size_t i) { return v < snaps_[i].session; });
        snap_pos_ = static_cast<std::size_t>(lo - order_.begin());
        snap_end_ = static_cast<std::size_t>(hi - order_.begin());
    }

    void finish_session() {
        if (unsynced_since_) cur_.unsynced_ns += cur_.last_rx - *unsynced_since_;
        unsynced_since_.reset();
        cur_.snaps_after_end = static_cast<std::int64_t>(snap_end_ - snap_pos_);
        results_.push_back(std::move(cur_));
        has_session_ = false;
    }

    std::size_t top_n_;
    bool profiling_;
    Profile profile_;
    std::vector<Snapshot> snaps_;
    std::vector<std::size_t> order_;           // snapshot indices sorted by (session, update id)
    std::vector<SessionResult> results_;       // finished sessions
    std::unordered_set<std::int64_t> seen_;    // sessions already started
    SessionResult cur_;
    bool has_session_ = false, started_ = false, synced_ = false;
    std::optional<std::int64_t> unsynced_since_;  // rx of the first event without a valid book
    std::size_t snap_pos_ = 0, snap_end_ = 0;      // next snapshot of the current session, end of its run
    Book book_;
    std::vector<Price> excl_bid_, excl_ask_;       // scratch buffers, reused to avoid allocations
    std::vector<Level> in_range_;
    std::vector<Mismatch> found_;
};

}  // namespace lob
