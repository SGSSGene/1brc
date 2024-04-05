#include <ivio/ivio.h>
#include <ivio/detail/mmap_reader.h>
#include <iostream>
#include <limits>
#include <unordered_map>
#include <map>
#include <charconv>
#include <thread>
#include <ankerl/unordered_dense.h>

/*struct string_hash
{
    using hash_type = std::hash<std::string_view>;
    using is_transparent = void;

    std::size_t operator()(const char* str) const        { return hash_type{}(str); }
    std::size_t operator()(std::string_view str) const   { return hash_type{}(str); }
    std::size_t operator()(std::string const& str) const { return hash_type{}(str); }
};*/
struct string_hash {
    using is_transparent = void; // enable heterogeneous overloads
    using is_avalanching = void; // mark class as high quality avalanching hash

    [[nodiscard]] auto operator()(std::string_view str) const noexcept -> uint64_t {
        return ankerl::unordered_dense::hash<std::string_view>{}(str);
    }
};

struct record {
    //double min{std::numeric_limits<double>::max()};
    //double max{std::numeric_limits<double>::lowest()};
    //double acc{};
    int min{std::numeric_limits<int>::max()};
    int max{std::numeric_limits<int>::lowest()};
    int acc{};

    int ct{};
    void update(int v) {
        min = std::min(min, v);
        max = std::max(max, v);
        acc += v;
        ct += 1;
    }
    void merge(record const& _other) {
        min = std::min(min, _other.min);
        max = std::max(max, _other.max);
        acc += _other.acc;
        ct += _other.ct;
    }
};

using Map = ankerl::unordered_dense::map<std::string, record, string_hash, std::equal_to<>>;

static auto const lut = []() {
    auto r = std::array<int, 256>{};
    for (auto i : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
        r['0' + i] = i;
    }
    r['.'] = 0;
    r['-'] = 0;
    return r;
}();


auto analyze(std::filesystem::path inputFile, size_t byteStart, size_t byteEnd) {
    //auto records = std::unordered_map<std::string, record, string_hash, std::equal_to<>>{};
    auto records = Map{};
    auto reader = ivio::mmap_reader{inputFile};

    size_t processedBytes{};
    size_t pos = byteStart;

    // Find a got starting position
    if (byteStart > 0
        and reader.string_view(byteStart-1, byteStart) != "\n") {
        pos = reader.readUntil('\n', pos) + 1;
        processedBytes = pos-byteStart;
    }
    reader.dropUntil(pos);
    pos = 0;

    std::cout << "start-end: " << byteStart << " - " << byteEnd << "\n";
    //std::cout << "processed: " << processedBytes << "\n";
    do {
        if (processedBytes+pos >= byteEnd-byteStart) break;
        if (pos >= 65536) {
            reader.dropUntil(pos);
            processedBytes += pos;
            pos = 0;
        }
        auto midPos = reader.readUntil(';', pos);
        auto commaPos = reader.readUntil('.', midPos+1);
        auto lineEnd = commaPos + 2;
//        auto lineEnd = reader.readUntil('\n', midPos+1);

        auto place_view = reader.string_view(pos, midPos);
        auto value1_view = reader.string_view(midPos+1, commaPos);
        auto value2_view = reader.string_view(commaPos+1, lineEnd);

//        auto [iter, s] = records.try_emplace(std::string{place_view});
        auto iter = records.find(place_view);
        if (iter == records.end()) {
            bool s;
            std::tie(iter, s) = records.try_emplace(std::string{place_view});
        }
        int v = 0;
        int sign = 1;
        if (value1_view[0] == '-') {
            sign = -1;
            value1_view.remove_prefix(1);
        }

        for (auto c : value1_view) {
            v = v*10 + c - '0';
        }
        
//        std::from_chars(value1_view.begin(), value1_view.end(), v);
        v = v*10 + value2_view[0] - '0';
/*        for (auto c : value1_view) {
            v = v*10 + lut[c];
        }
        v = v*10 + lut[value2_view[0]];*/
        v = v * sign;

/*        int v;
        std::from_chars(value1_view.begin(), value1_view.end(), v2);
        int v = v2 * 10.;*/
        iter->second.update(v);
        pos = lineEnd+1;
    } while (!reader.eof(pos));

    return records;
}



int main(int argc, char** argv) {
    if (argc < 2) return 1;

    auto inputFile = std::filesystem::path{argv[1]};
    size_t fileSize = std::filesystem::file_size(inputFile);

    size_t threadCt = 4;
    auto threads = std::vector<std::jthread>{};
    auto resultList = std::vector<Map>{};
    resultList.resize(threadCt);
    for (size_t i{0}; i < threadCt; ++i) {
        threads.emplace_back([&, i]() {
            auto records = analyze(inputFile, (fileSize*i)/threadCt , fileSize*(i+1)/threadCt);
            resultList[i] = std::move(records);
        });
    }
    threads.clear();
/*
    for (size_t i{0}; i < threadCt; ++i) {
//        auto r = record{};
        for (auto const& [key, r] : resultList[i]) {
            std::cout << i << " " << key << " (min/max/avg/ct): " << r.min << "/" << r.max << "/" << r.acc << "/" << r.ct << "\n";
//            r.merge(v);
        }
//        std::cout << i << " (min/max/avg/ct): " << r.min << "/" << r.max << "/" << r.acc/r.ct << "/" << r.ct << "\n";
    }
*/
    for (size_t i{1}; i < threadCt; ++i) {
        for (auto const& [key, v] : resultList[i]) {
            resultList[0][key].merge(v);
        }
    }

    auto r = record{};
    for (auto const& [key, v] : resultList[0]) {
        r.merge(v);
    }
    std::cout << "(min/max/avg/ct): " << r.min << "/" << r.max << "/" << r.acc/r.ct << "/" << r.ct << "\n";

    //for (auto const& [key, r] : records) {
//        std::cout << key << "(min/max/avg/ct): " << r.min << "/" << r.max << "/" << r.acc/r.ct << "/" << r.ct << "\n";
    //}
}
