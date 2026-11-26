// bindings.cpp: the Python module 'lobcpp', exposing book.hpp and replay.hpp with pybind11.
//
//     import lobcpp
//     b = lobcpp.VecBook()            # or lobcpp.MapBook()
//     b.load_snapshot(last_update_id, bid_prices, bid_qtys, ask_prices, ask_qtys)
//     b.apply(sides, prices, qtys, last_id)   # one depth event; numpy arrays or lists of ints
//     b.best(0), b.top(0, 10), b.size(1), b.crossed(), b.last_update_id
//
//     r = lobcpp.VecReplayer(top_n)   # or lobcpp.MapReplayer: the replay.py state machine
//     r.set_snapshots(rx, session, last_id, offsets, side, price, qty)      # all snapshots, once
//     out = r.process(rx, session, first_id, last_id, offsets, side, price, qty)  # e.g. one hour
//     r.finish(); r.sessions()        # per-session results as dicts
//     (bookdata.py builds these arrays from the Parquet tables.)
//
// Prices are integer ticks and quantities integer lots (see book.hpp). Arrays are passed without
// copying when they already are C-contiguous int8 / int64 numpy arrays; anything else (a list,
// another dtype) is converted first ('forcecast').
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "lob/book.hpp"
#include "lob/replay.hpp"

namespace py = pybind11;

using I8 = py::array_t<std::int8_t, py::array::c_style | py::array::forcecast>;
using I64 = py::array_t<std::int64_t, py::array::c_style | py::array::forcecast>;
using PriceQty = std::pair<lob::Price, lob::Qty>;

static lob::Side to_side(int side) {
    if (side != lob::kBid && side != lob::kAsk) throw std::invalid_argument("side must be 0 (bid) or 1 (ask)");
    return static_cast<lob::Side>(side);
}

static std::vector<lob::Level> to_levels(const I64& price, const I64& qty) {
    if (price.size() != qty.size()) throw std::invalid_argument("prices and quantities differ in length");
    std::vector<lob::Level> out(static_cast<std::size_t>(price.size()));
    for (std::size_t i = 0; i < out.size(); ++i) out[i] = {price.data()[i], qty.data()[i]};
    return out;
}

template <class Book>
static void bind_book(py::module_& m, const char* name) {
    py::class_<Book>(m, name)
        .def(py::init<>())
        .def("set", [](Book& b, int side, lob::Price p, lob::Qty q) { return b.set(to_side(side), p, q); },
             "Set one level (qty 0 removes it); False if a missing level was to be removed.")
        .def(
            "apply",
            [](Book& b, const I8& side, const I64& price, const I64& qty, std::int64_t last_id) {
                if (price.size() != side.size() || qty.size() != side.size())
                    throw std::invalid_argument("side, price and qty must have the same length");
                for (py::ssize_t i = 0; i < side.size(); ++i) to_side(side.data()[i]);  // validate first
                return b.apply(side.data(), price.data(), qty.data(), static_cast<std::size_t>(side.size()),
                               last_id);
            },
            "Apply one depth event; returns the number of removals of missing levels.")
        .def(
            "load_snapshot",
            [](Book& b, std::int64_t last_id, const I64& bp, const I64& bq, const I64& ap, const I64& aq) {
                b.load_snapshot(last_id, to_levels(bp, bq), to_levels(ap, aq));
            },
            "Replace the book with a snapshot.")
        .def(
            "best",
            [](const Book& b, int side) -> std::optional<PriceQty> {
                const std::optional<lob::Level> l = to_side(side) == lob::kBid ? b.bids.best() : b.asks.best();
                if (!l) return std::nullopt;
                return PriceQty{l->price, l->qty};
            },
            "Best level of a side as (price, qty), or None if the side is empty.")
        .def(
            "top",
            [](const Book& b, int side, std::size_t n) {
                std::vector<lob::Level> levels;
                if (to_side(side) == lob::kBid) b.bids.top(n, levels);
                else b.asks.top(n, levels);
                std::vector<PriceQty> out;
                out.reserve(levels.size());
                for (const lob::Level& l : levels) out.emplace_back(l.price, l.qty);
                return out;
            },
            "The n best levels of a side as [(price, qty), ...], best first.")
        .def("size", [](const Book& b, int side) { return to_side(side) == lob::kBid ? b.bids.size() : b.asks.size(); })
        .def("crossed", &Book::crossed)
        .def("clear", &Book::clear)
        .def_readwrite("last_update_id", &Book::last_update_id);
}

// Arrays in "CSR" layout: item i owns the levels [offsets[i], offsets[i + 1]) of side / price / qty.
// Checked once here so that the C++ loops can trust them.
static void check_csr(py::ssize_t n_items, const I64& offsets, const I8& side, const I64& price, const I64& qty) {
    const py::ssize_t m = side.size();
    if (offsets.size() != n_items + 1) throw std::invalid_argument("offsets must have one more element than the items");
    if (price.size() != m || qty.size() != m) throw std::invalid_argument("side, price and qty must have the same length");
    const std::int64_t* o = offsets.data();
    if (o[0] != 0 || o[n_items] != m) throw std::invalid_argument("offsets must start at 0 and end at the number of levels");
    for (py::ssize_t i = 0; i < n_items; ++i)
        if (o[i + 1] < o[i]) throw std::invalid_argument("offsets must not decrease");
    for (py::ssize_t k = 0; k < m; ++k) to_side(side.data()[k]);
}

static void check_len(py::ssize_t n, std::initializer_list<const I64*> arrays) {
    for (const I64* a : arrays)
        if (a->size() != n) throw std::invalid_argument("per-item arrays must have the same length");
}

static py::dict session_to_dict(const lob::SessionResult& s) {
    py::list gaps, syncs, checks;
    for (const lob::Gap& g : s.gaps) gaps.append(py::make_tuple(g.rx_ns, g.expected_first_id, g.got_first_id));
    for (const lob::Sync& y : s.syncs) syncs.append(py::make_tuple(y.snap_rx_ns, y.event_rx_ns, y.last_update_id));
    for (const lob::Check& c : s.checks) {
        py::list mm;
        for (const lob::Mismatch& x : c.mismatches)
            mm.append(py::make_tuple(x.side, x.price, x.book_qty, x.snap_qty, x.untracked));
        py::dict d;
        d["snap_rx_ns"] = c.snap_rx_ns;
        d["last_update_id"] = c.last_update_id;
        d["straddle"] = c.straddle;
        d["compared"] = c.compared;
        d["excluded"] = c.excluded;
        d["mismatches"] = mm;
        d["crossed"] = c.crossed;
        d["book_levels"] = c.book_levels;
        checks.append(d);
    }
    py::dict d;
    d["session"] = s.session;
    d["first_rx"] = s.first_rx;
    d["last_rx"] = s.last_rx;
    d["events"] = s.events;
    d["applied"] = s.applied;
    d["stale"] = s.stale;
    d["unsynced_events"] = s.unsynced_events;
    d["removed_missing"] = s.removed_missing;
    d["unusable_snaps"] = s.unusable_snaps;
    d["skipped_snaps"] = s.skipped_snaps;
    d["snaps_after_end"] = s.snaps_after_end;
    d["unsynced_ns"] = s.unsynced_ns;
    d["gaps"] = gaps;
    d["syncs"] = syncs;
    d["checks"] = checks;
    d["bid_floor"] = s.bid_floor ? py::object(py::int_(*s.bid_floor)) : py::object(py::none());
    d["ask_cap"] = s.ask_cap ? py::object(py::int_(*s.ask_cap)) : py::object(py::none());
    return d;
}

template <class Book>
static void bind_replayer(py::module_& m, const char* name) {
    using R = lob::Replayer<Book>;
    py::class_<R>(m, name)
        .def(py::init<std::size_t, bool>(), py::arg("top_n") = 100, py::arg("profile") = false,
             "profile=True records where each level update lands (slower; for bench.py)")
        .def(
            "set_snapshots",
            [](R& r, const I64& rx, const I64& session, const I64& last_id, const I64& offsets, const I8& side,
               const I64& price, const I64& qty) {
                const py::ssize_t n = rx.size();
                check_len(n, {&session, &last_id});
                check_csr(n, offsets, side, price, qty);
                r.set_snapshots(static_cast<std::size_t>(n), rx.data(), session.data(), last_id.data(),
                                offsets.data(), side.data(), price.data(), qty.data());
            },
            "All REST snapshots, in receive order, before the first event.")
        .def(
            "process",
            [](R& r, const I64& rx, const I64& session, const I64& first_id, const I64& last_id, const I64& offsets,
               const I8& side, const I64& price, const I64& qty, bool timings) {
                const py::ssize_t n = rx.size();
                check_len(n, {&session, &first_id, &last_id});
                check_csr(n, offsets, side, price, qty);
                py::array_t<std::int8_t> status(n);
                py::array_t<std::int64_t> bid_px(n), bid_qty(n), ask_px(n), ask_qty(n), ns(timings ? n : 0);
                r.process(static_cast<std::size_t>(n), rx.data(), session.data(), first_id.data(), last_id.data(),
                          offsets.data(), side.data(), price.data(), qty.data(), status.mutable_data(),
                          bid_px.mutable_data(), bid_qty.mutable_data(), ask_px.mutable_data(), ask_qty.mutable_data(),
                          timings ? ns.mutable_data() : nullptr);
                py::dict out;
                out["status"] = status;  // 0 applied, 1 not synced, 2 stale
                out["bid_px"] = bid_px;  // top of book after each event; 0 = none
                out["bid_qty"] = bid_qty;
                out["ask_px"] = ask_px;
                out["ask_qty"] = ask_qty;
                if (timings) out["ns"] = ns;  // time spent in each event, nanoseconds
                return out;
            },
            py::arg("rx"), py::arg("session"), py::arg("first_id"), py::arg("last_id"), py::arg("offsets"),
            py::arg("side"), py::arg("price"), py::arg("qty"), py::arg("timings") = false,
            "Process consecutive depth events; returns per-event status and top of book.")
        .def("finish", &R::finish, "Close the current session (after the last event).")
        .def(
            "profile",
            [](const R& r) {
                const lob::Profile& p = r.profile();
                py::dict d;
                d["inserts"] = p.inserts;
                d["erases"] = p.erases;
                d["in_place"] = p.in_place;
                d["missing"] = p.missing;
                d["shifted"] = p.shifted;
                d["by_depth"] = std::vector<std::int64_t>(p.by_depth.begin(), p.by_depth.end());
                d["events"] = p.events;
                d["bid_levels"] = p.bid_levels;
                d["ask_levels"] = p.ask_levels;
                d["behind"] = std::vector<std::int64_t>(p.behind.begin(), p.behind.end());
                d["ahead"] = std::vector<std::int64_t>(p.ahead.begin(), p.ahead.end());
                d["no_best"] = p.no_best;
                d["next_gap"] = std::vector<std::int64_t>(p.next_gap.begin(), p.next_gap.end());
                d["last_level"] = p.last_level;
                return d;
            },
            "Counts recorded when constructed with profile=True.")
        .def(
            "sessions",
            [](const R& r) {
                py::list out;
                for (const lob::SessionResult& s : r.sessions()) out.append(session_to_dict(s));
                return out;
            },
            "Finished sessions as dicts (same fields as replay.SessionReplay; prices in ticks, qty in lots).");
}

PYBIND11_MODULE(lobcpp, m) {
    m.doc() = "C++ L2 order book and replay engine with integer ticks and lots (see cpp/include/lob/)";
    bind_book<lob::VecBook>(m, "VecBook");
    bind_book<lob::MapBook>(m, "MapBook");
    bind_replayer<lob::VecBook>(m, "VecReplayer");
    bind_replayer<lob::MapBook>(m, "MapReplayer");

    // Smallest step of steady_clock seen in practice (it bounds the precision of per-event timings).
    m.def("clock_resolution_ns", []() {
        using clock = std::chrono::steady_clock;
        std::int64_t best = std::numeric_limits<std::int64_t>::max();
        for (int i = 0; i < 20000; ++i) {
            const auto t0 = clock::now();
            auto t1 = clock::now();
            while (t1 == t0) t1 = clock::now();
            best = std::min<std::int64_t>(best, std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        }
        return best;
    });
#if defined(_MSC_VER)
    m.attr("compiler") = "MSVC " + std::to_string(_MSC_FULL_VER);
#elif defined(__clang__)
    m.attr("compiler") = std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
    m.attr("compiler") = std::string("gcc ") + __VERSION__;
#else
    m.attr("compiler") = "unknown";
#endif
#ifdef NDEBUG
    m.attr("build") = "release";  // optimized, asserts off
#else
    m.attr("build") = "debug";
#endif
}
