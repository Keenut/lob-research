// bindings.cpp: the Python module 'lobcpp', exposing the C++ books of book.hpp with pybind11.
//
//     import lobcpp
//     b = lobcpp.VecBook()            # or lobcpp.MapBook()
//     b.load_snapshot(last_update_id, bid_prices, bid_qtys, ask_prices, ask_qtys)
//     b.apply(sides, prices, qtys, last_id)   # one depth event; numpy arrays or lists of ints
//     b.best(0), b.top(0, 10), b.size(1), b.crossed(), b.last_update_id
//
// Prices are integer ticks and quantities integer lots (see book.hpp). Arrays are passed without
// copying when they already are C-contiguous int8 / int64 numpy arrays; anything else (a list,
// another dtype) is converted first ('forcecast').
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "lob/book.hpp"

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

PYBIND11_MODULE(lobcpp, m) {
    m.doc() = "C++ L2 order book with integer ticks and lots (see cpp/include/lob/book.hpp)";
    bind_book<lob::VecBook>(m, "VecBook");
    bind_book<lob::MapBook>(m, "MapBook");
}