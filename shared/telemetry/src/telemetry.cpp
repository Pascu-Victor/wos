#include <charconv>
#include <limits>
#include <utility>
#include <wos/telemetry.hpp>

namespace wos::telemetry {
namespace {

constexpr size_t CONTAINER_HEADER_SIZE = 40;
constexpr size_t SECTION_HEADER_SIZE = 24;
constexpr size_t HEADER_CRC_OFFSET = 32;

auto set_error(Error& error, ErrorCode code, size_t offset, std::string message) -> void {
    if (!error) {
        error = Error{code, offset, std::move(message)};
    }
}

auto checked_add(size_t left, size_t right, size_t& result) -> bool {
    if (left > std::numeric_limits<size_t>::max() - right) {
        return false;
    }
    result = left + right;
    return true;
}

auto is_continuation(unsigned char byte) -> bool { return (byte & 0xc0U) == 0x80U; }

auto valid_utf8_sequence(std::string_view input, size_t offset, size_t& width) -> bool {
    const auto FIRST = static_cast<unsigned char>(input[offset]);
    if (FIRST < 0x80U) {
        width = 1;
        return true;
    }
    if (FIRST >= 0xc2U && FIRST <= 0xdfU) {
        width = 2;
    } else if (FIRST >= 0xe0U && FIRST <= 0xefU) {
        width = 3;
    } else if (FIRST >= 0xf0U && FIRST <= 0xf4U) {
        width = 4;
    } else {
        return false;
    }
    if (offset + width > input.size()) {
        return false;
    }
    for (size_t i = 1; i < width; ++i) {
        if (!is_continuation(static_cast<unsigned char>(input[offset + i]))) {
            return false;
        }
    }
    if (width == 3) {
        const auto SECOND = static_cast<unsigned char>(input[offset + 1]);
        if ((FIRST == 0xe0U && SECOND < 0xa0U) || (FIRST == 0xedU && SECOND >= 0xa0U)) {
            return false;
        }
    } else if (width == 4) {
        const auto SECOND = static_cast<unsigned char>(input[offset + 1]);
        if ((FIRST == 0xf0U && SECOND < 0x90U) || (FIRST == 0xf4U && SECOND >= 0x90U)) {
            return false;
        }
    }
    return true;
}

auto append_utf8(std::string& output, uint32_t codepoint) -> void {
    if (codepoint <= 0x7fU) {
        output.push_back(static_cast<char>(codepoint));
    } else if (codepoint <= 0x7ffU) {
        output.push_back(static_cast<char>(0xc0U | (codepoint >> 6U)));
        output.push_back(static_cast<char>(0x80U | (codepoint & 0x3fU)));
    } else if (codepoint <= 0xffffU) {
        output.push_back(static_cast<char>(0xe0U | (codepoint >> 12U)));
        output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3fU)));
        output.push_back(static_cast<char>(0x80U | (codepoint & 0x3fU)));
    } else {
        output.push_back(static_cast<char>(0xf0U | (codepoint >> 18U)));
        output.push_back(static_cast<char>(0x80U | ((codepoint >> 12U) & 0x3fU)));
        output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3fU)));
        output.push_back(static_cast<char>(0x80U | (codepoint & 0x3fU)));
    }
}

class Parser {
   public:
    Parser(std::string_view input, Limits limits) : input_(input), limits_(limits) {}

    auto run() -> ParseResult {
        if (input_.size() > limits_.max_input_bytes) {
            return {{}, {ErrorCode::INPUT_TOO_LARGE, 0, "JSON input exceeds the configured byte limit"}};
        }
        skip_space();
        auto value = parse_value(0);
        if (!value) {
            return {{}, std::move(error_)};
        }
        skip_space();
        if (offset_ != input_.size()) {
            return {{}, {ErrorCode::TRAILING_DATA, offset_, "trailing data after JSON value"}};
        }
        return {std::move(value), {}};
    }

   private:
    auto skip_space() -> void {
        while (offset_ < input_.size()) {
            const char CH = input_[offset_];
            if (CH != ' ' && CH != '\t' && CH != '\r' && CH != '\n') {
                break;
            }
            ++offset_;
        }
    }

    auto consume(char expected) -> bool {
        if (offset_ >= input_.size() || input_[offset_] != expected) {
            return false;
        }
        ++offset_;
        return true;
    }

    auto count_node() -> bool {
        if (++nodes_ > limits_.max_nodes) {
            set_error(error_, ErrorCode::NODE_LIMIT, offset_, "JSON node count exceeds the configured limit");
            return false;
        }
        return true;
    }

    auto parse_value(size_t depth) -> std::optional<Value> {
        if (depth > limits_.max_depth) {
            set_error(error_, ErrorCode::DEPTH_LIMIT, offset_, "JSON nesting exceeds the configured depth limit");
            return std::nullopt;
        }
        if (!count_node()) {
            return std::nullopt;
        }
        if (offset_ >= input_.size()) {
            set_error(error_, ErrorCode::UNEXPECTED_END, offset_, "unexpected end of JSON input");
            return std::nullopt;
        }
        switch (input_[offset_]) {
            case 'n':
                return parse_literal("null", Value(nullptr));
            case 't':
                return parse_literal("true", Value(true));
            case 'f':
                return parse_literal("false", Value(false));
            case '"': {
                auto string = parse_string();
                if (!string) {
                    return std::nullopt;
                }
                return Value(std::move(*string));
            }
            case '[':
                return parse_array(depth + 1);
            case '{':
                return parse_object(depth + 1);
            default:
                if (input_[offset_] == '-' || (input_[offset_] >= '0' && input_[offset_] <= '9')) {
                    return parse_number();
                }
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "invalid JSON value");
                return std::nullopt;
        }
    }

    auto parse_literal(std::string_view literal, Value value) -> std::optional<Value> {
        if (input_.substr(offset_, literal.size()) != literal) {
            set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "invalid JSON literal");
            return std::nullopt;
        }
        offset_ += literal.size();
        return value;
    }

    auto parse_hex_quad(uint32_t& codepoint) -> bool {
        if (offset_ + 4 > input_.size()) {
            set_error(error_, ErrorCode::UNEXPECTED_END, offset_, "truncated JSON unicode escape");
            return false;
        }
        codepoint = 0;
        for (size_t i = 0; i < 4; ++i) {
            const char CH = input_[offset_++];
            uint32_t nibble = 0;
            if (CH >= '0' && CH <= '9') {
                nibble = static_cast<uint32_t>(CH - '0');
            } else if (CH >= 'a' && CH <= 'f') {
                nibble = static_cast<uint32_t>(CH - 'a' + 10);
            } else if (CH >= 'A' && CH <= 'F') {
                nibble = static_cast<uint32_t>(CH - 'A' + 10);
            } else {
                set_error(error_, ErrorCode::INVALID_ESCAPE, offset_ - 1, "invalid hexadecimal digit in JSON escape");
                return false;
            }
            codepoint = (codepoint << 4U) | nibble;
        }
        return true;
    }

    auto parse_string() -> std::optional<std::string> {
        const size_t START = offset_;
        ++offset_;  // opening quote
        std::string output;
        while (offset_ < input_.size()) {
            const auto CH = static_cast<unsigned char>(input_[offset_++]);
            if (CH == '"') {
                return output;
            }
            if (CH < 0x20U) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_ - 1, "unescaped control byte in JSON string");
                return std::nullopt;
            }
            if (CH == '\\') {
                if (offset_ >= input_.size()) {
                    set_error(error_, ErrorCode::UNEXPECTED_END, offset_, "truncated JSON escape");
                    return std::nullopt;
                }
                const char ESCAPE = input_[offset_++];
                switch (ESCAPE) {
                    case '"':
                        output.push_back('"');
                        break;
                    case '\\':
                        output.push_back('\\');
                        break;
                    case '/':
                        output.push_back('/');
                        break;
                    case 'b':
                        output.push_back('\b');
                        break;
                    case 'f':
                        output.push_back('\f');
                        break;
                    case 'n':
                        output.push_back('\n');
                        break;
                    case 'r':
                        output.push_back('\r');
                        break;
                    case 't':
                        output.push_back('\t');
                        break;
                    case 'u': {
                        uint32_t codepoint = 0;
                        if (!parse_hex_quad(codepoint)) {
                            return std::nullopt;
                        }
                        if (codepoint >= 0xd800U && codepoint <= 0xdbffU) {
                            if (offset_ + 2 > input_.size() || input_[offset_] != '\\' || input_[offset_ + 1] != 'u') {
                                set_error(error_, ErrorCode::INVALID_ESCAPE, offset_, "high surrogate without low surrogate");
                                return std::nullopt;
                            }
                            offset_ += 2;
                            uint32_t low = 0;
                            if (!parse_hex_quad(low) || low < 0xdc00U || low > 0xdfffU) {
                                set_error(error_, ErrorCode::INVALID_ESCAPE, offset_, "invalid low surrogate");
                                return std::nullopt;
                            }
                            codepoint = 0x10000U + ((codepoint - 0xd800U) << 10U) + (low - 0xdc00U);
                        } else if (codepoint >= 0xdc00U && codepoint <= 0xdfffU) {
                            set_error(error_, ErrorCode::INVALID_ESCAPE, offset_, "unpaired low surrogate");
                            return std::nullopt;
                        }
                        append_utf8(output, codepoint);
                        break;
                    }
                    default:
                        set_error(error_, ErrorCode::INVALID_ESCAPE, offset_ - 1, "invalid JSON escape");
                        return std::nullopt;
                }
            } else if (CH < 0x80U) {
                output.push_back(static_cast<char>(CH));
            } else {
                --offset_;
                size_t width = 0;
                if (!valid_utf8_sequence(input_, offset_, width)) {
                    set_error(error_, ErrorCode::INVALID_UTF8, offset_, "invalid UTF-8 in JSON string");
                    return std::nullopt;
                }
                output.append(input_.substr(offset_, width));
                offset_ += width;
            }
            if (output.size() > limits_.max_string_bytes) {
                set_error(error_, ErrorCode::STRING_LIMIT, START, "JSON string exceeds the configured byte limit");
                return std::nullopt;
            }
        }
        set_error(error_, ErrorCode::UNEXPECTED_END, offset_, "unterminated JSON string");
        return std::nullopt;
    }

    auto parse_number() -> std::optional<Value> {
        const size_t START = offset_;
        consume('-');
        if (offset_ >= input_.size()) {
            set_error(error_, ErrorCode::UNEXPECTED_END, offset_, "truncated JSON number");
            return std::nullopt;
        }
        if (consume('0')) {
            if (offset_ < input_.size() && input_[offset_] >= '0' && input_[offset_] <= '9') {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "leading zero in JSON number");
                return std::nullopt;
            }
        } else {
            if (input_[offset_] < '1' || input_[offset_] > '9') {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "invalid JSON number");
                return std::nullopt;
            }
            while (offset_ < input_.size() && input_[offset_] >= '0' && input_[offset_] <= '9') {
                ++offset_;
            }
        }
        if (consume('.')) {
            const size_t FRACTION = offset_;
            while (offset_ < input_.size() && input_[offset_] >= '0' && input_[offset_] <= '9') {
                ++offset_;
            }
            if (offset_ == FRACTION) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "missing JSON fractional digits");
                return std::nullopt;
            }
        }
        if (offset_ < input_.size() && (input_[offset_] == 'e' || input_[offset_] == 'E')) {
            ++offset_;
            if (offset_ < input_.size() && (input_[offset_] == '+' || input_[offset_] == '-')) {
                ++offset_;
            }
            const size_t EXPONENT = offset_;
            while (offset_ < input_.size() && input_[offset_] >= '0' && input_[offset_] <= '9') {
                ++offset_;
            }
            if (offset_ == EXPONENT) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "missing JSON exponent digits");
                return std::nullopt;
            }
        }
        return Value::number(std::string(input_.substr(START, offset_ - START)));
    }

    auto parse_array(size_t depth) -> std::optional<Value> {
        ++offset_;
        skip_space();
        Value::Array values;
        if (consume(']')) {
            return Value(std::move(values));
        }
        while (true) {
            if (values.size() >= limits_.max_array_elements) {
                set_error(error_, ErrorCode::ARRAY_LIMIT, offset_, "JSON array exceeds the configured element limit");
                return std::nullopt;
            }
            auto value = parse_value(depth);
            if (!value) {
                return std::nullopt;
            }
            values.push_back(std::move(*value));
            skip_space();
            if (consume(']')) {
                return Value(std::move(values));
            }
            if (!consume(',')) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "expected comma or closing bracket");
                return std::nullopt;
            }
            skip_space();
        }
    }

    auto parse_object(size_t depth) -> std::optional<Value> {
        ++offset_;
        skip_space();
        Value::Object values;
        if (consume('}')) {
            return Value(std::move(values));
        }
        while (true) {
            if (values.size() >= limits_.max_object_members) {
                set_error(error_, ErrorCode::MEMBER_LIMIT, offset_, "JSON object exceeds the configured member limit");
                return std::nullopt;
            }
            if (offset_ >= input_.size() || input_[offset_] != '"') {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "expected JSON object key");
                return std::nullopt;
            }
            auto key = parse_string();
            if (!key) {
                return std::nullopt;
            }
            skip_space();
            if (!consume(':')) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "expected colon after JSON object key");
                return std::nullopt;
            }
            skip_space();
            auto value = parse_value(depth);
            if (!value) {
                return std::nullopt;
            }
            auto [_, inserted] = values.emplace(std::move(*key), std::move(*value));
            if (!inserted) {
                set_error(error_, ErrorCode::DUPLICATE_KEY, offset_, "duplicate JSON object key");
                return std::nullopt;
            }
            skip_space();
            if (consume('}')) {
                return Value(std::move(values));
            }
            if (!consume(',')) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, offset_, "expected comma or closing brace");
                return std::nullopt;
            }
            skip_space();
        }
    }

    std::string_view input_;
    Limits limits_;
    size_t offset_{0};
    size_t nodes_{0};
    Error error_;
};

class Serializer {
   public:
    explicit Serializer(Limits limits) : limits_(limits) {}

    auto run(const Value& value) -> SerializeResult {
        if (!append_value(value, 0)) {
            return {{}, std::move(error_)};
        }
        return {std::move(output_), {}};
    }

   private:
    auto append(std::string_view value) -> bool {
        size_t next = 0;
        if (!checked_add(output_.size(), value.size(), next) || next > limits_.max_input_bytes) {
            set_error(error_, ErrorCode::OUTPUT_TOO_LARGE, output_.size(), "serialized JSON exceeds the configured byte limit");
            return false;
        }
        output_.append(value);
        return true;
    }

    auto append_char(char value) -> bool { return append(std::string_view(&value, 1)); }

    auto append_string(std::string_view value) -> bool {
        if (value.size() > limits_.max_string_bytes || !append_char('"')) {
            if (!error_) {
                set_error(error_, ErrorCode::STRING_LIMIT, output_.size(), "JSON string exceeds the configured byte limit");
            }
            return false;
        }
        static constexpr char HEX[] = "0123456789abcdef";
        for (size_t i = 0; i < value.size();) {
            const auto CH = static_cast<unsigned char>(value[i]);
            switch (CH) {
                case '"':
                    if (!append("\\\"")) return false;
                    ++i;
                    continue;
                case '\\':
                    if (!append("\\\\")) return false;
                    ++i;
                    continue;
                case '\b':
                    if (!append("\\b")) return false;
                    ++i;
                    continue;
                case '\f':
                    if (!append("\\f")) return false;
                    ++i;
                    continue;
                case '\n':
                    if (!append("\\n")) return false;
                    ++i;
                    continue;
                case '\r':
                    if (!append("\\r")) return false;
                    ++i;
                    continue;
                case '\t':
                    if (!append("\\t")) return false;
                    ++i;
                    continue;
                default:
                    break;
            }
            if (CH < 0x20U) {
                char escaped[] = {'\\', 'u', '0', '0', HEX[CH >> 4U], HEX[CH & 0xfU]};
                if (!append(std::string_view(escaped, sizeof(escaped)))) {
                    return false;
                }
                ++i;
                continue;
            }
            size_t width = 0;
            if (!valid_utf8_sequence(value, i, width)) {
                set_error(error_, ErrorCode::INVALID_UTF8, i, "cannot serialize invalid UTF-8 string");
                return false;
            }
            if (!append(value.substr(i, width))) {
                return false;
            }
            i += width;
        }
        return append_char('"');
    }

    auto count_node(size_t depth) -> bool {
        if (depth > limits_.max_depth) {
            set_error(error_, ErrorCode::DEPTH_LIMIT, output_.size(), "JSON nesting exceeds the configured depth limit");
            return false;
        }
        if (++nodes_ > limits_.max_nodes) {
            set_error(error_, ErrorCode::NODE_LIMIT, output_.size(), "JSON node count exceeds the configured limit");
            return false;
        }
        return true;
    }

    auto append_value(const Value& value, size_t depth) -> bool {
        if (!count_node(depth)) {
            return false;
        }
        if (value.is_null()) {
            return append("null");
        }
        if (const auto* boolean = value.as_bool()) {
            return append(*boolean ? "true" : "false");
        }
        if (const auto* number = value.as_number()) {
            auto parsed = parse(number->lexeme, Limits{number->lexeme.size(), 1, 1, 0, 0, 0});
            if (!parsed || !parsed.value->is_number()) {
                set_error(error_, ErrorCode::INVALID_SYNTAX, output_.size(), "invalid JSON number value");
                return false;
            }
            return append(number->lexeme);
        }
        if (const auto* string = value.as_string()) {
            return append_string(*string);
        }
        if (const auto* array = value.as_array()) {
            if (array->size() > limits_.max_array_elements || !append_char('[')) {
                if (!error_) set_error(error_, ErrorCode::ARRAY_LIMIT, output_.size(), "JSON array exceeds the configured element limit");
                return false;
            }
            for (size_t i = 0; i < array->size(); ++i) {
                if ((i != 0 && !append_char(',')) || !append_value((*array)[i], depth + 1)) {
                    return false;
                }
            }
            return append_char(']');
        }
        const auto* object = value.as_object();
        if (object == nullptr || object->size() > limits_.max_object_members || !append_char('{')) {
            if (!error_) set_error(error_, ErrorCode::MEMBER_LIMIT, output_.size(), "JSON object exceeds the configured member limit");
            return false;
        }
        size_t index = 0;
        for (const auto& [key, child] : *object) {
            if ((index++ != 0 && !append_char(',')) || !append_string(key) || !append_char(':') || !append_value(child, depth + 1)) {
                return false;
            }
        }
        return append_char('}');
    }

    Limits limits_;
    size_t nodes_{0};
    std::string output_;
    Error error_;
};

template <typename T>
auto append_le(std::vector<std::byte>& output, T value) -> void {
    for (size_t i = 0; i < sizeof(T); ++i) {
        output.push_back(static_cast<std::byte>((static_cast<uint64_t>(value) >> (i * 8U)) & 0xffU));
    }
}

template <typename T>
auto read_le(std::span<const std::byte> input, size_t offset) -> T {
    uint64_t result = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        result |= static_cast<uint64_t>(std::to_integer<unsigned char>(input[offset + i])) << (i * 8U);
    }
    return static_cast<T>(result);
}

auto crc_with_zeroed_header_field(std::span<const std::byte> header) -> uint32_t {
    std::vector<std::byte> copy(header.begin(), header.end());
    for (size_t i = HEADER_CRC_OFFSET; i < HEADER_CRC_OFFSET + sizeof(uint32_t); ++i) {
        copy[i] = std::byte{0};
    }
    return crc32c(copy);
}

}  // namespace

auto Value::number(std::string lexeme) -> Value {
    Value value;
    value.data = Number{std::move(lexeme)};
    return value;
}

auto Value::unsigned_integer(uint64_t value) -> Value { return Value(decimal_u64_string(value)); }
auto Value::signed_integer(int64_t value) -> Value { return Value(decimal_i64_string(value)); }
auto Value::is_null() const -> bool { return std::holds_alternative<std::nullptr_t>(data); }
auto Value::is_bool() const -> bool { return std::holds_alternative<bool>(data); }
auto Value::is_number() const -> bool { return std::holds_alternative<Number>(data); }
auto Value::is_string() const -> bool { return std::holds_alternative<std::string>(data); }
auto Value::is_array() const -> bool { return std::holds_alternative<Array>(data); }
auto Value::is_object() const -> bool { return std::holds_alternative<Object>(data); }
auto Value::as_bool() const -> const bool* { return std::get_if<bool>(&data); }
auto Value::as_number() const -> const Number* { return std::get_if<Number>(&data); }
auto Value::as_string() const -> const std::string* { return std::get_if<std::string>(&data); }
auto Value::as_array() const -> const Array* { return std::get_if<Array>(&data); }
auto Value::as_object() const -> const Object* { return std::get_if<Object>(&data); }
auto Value::as_array() -> Array* { return std::get_if<Array>(&data); }
auto Value::as_object() -> Object* { return std::get_if<Object>(&data); }

auto parse(std::string_view input, Limits limits) -> ParseResult { return Parser(input, limits).run(); }
auto serialize(const Value& value, Limits limits) -> SerializeResult { return Serializer(limits).run(value); }

auto object_member(const Value& value, std::string_view name) -> const Value* {
    const auto* object = value.as_object();
    if (object == nullptr) {
        return nullptr;
    }
    const auto found = object->find(name);
    return found == object->end() ? nullptr : &found->second;
}

auto object_string(const Value& value, std::string_view name) -> std::optional<std::string_view> {
    const auto* member = object_member(value, name);
    if (member == nullptr || member->as_string() == nullptr) {
        return std::nullopt;
    }
    return *member->as_string();
}

auto decimal_u64_string(uint64_t value) -> std::string {
    std::array<char, std::numeric_limits<uint64_t>::digits10 + 2> buffer{};
    const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    return error == std::errc{} ? std::string(buffer.data(), end) : std::string{};
}

auto decimal_i64_string(int64_t value) -> std::string {
    std::array<char, std::numeric_limits<int64_t>::digits10 + 3> buffer{};
    const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    return error == std::errc{} ? std::string(buffer.data(), end) : std::string{};
}

auto decimal_u64(const Value& value) -> std::optional<uint64_t> {
    const auto* text = value.as_string();
    if (text == nullptr || text->empty()) {
        return std::nullopt;
    }
    uint64_t result = 0;
    const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), result);
    if (error != std::errc{} || end != text->data() + text->size()) {
        return std::nullopt;
    }
    return result;
}

auto decimal_i64(const Value& value) -> std::optional<int64_t> {
    const auto* text = value.as_string();
    if (text == nullptr || text->empty()) {
        return std::nullopt;
    }
    int64_t result = 0;
    const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), result);
    if (error != std::errc{} || end != text->data() + text->size()) {
        return std::nullopt;
    }
    return result;
}

auto validate_envelope(const Value& value, Error* error) -> bool {
    auto fail = [&](ErrorCode code, std::string message) {
        if (error != nullptr) {
            *error = Error{code, 0, std::move(message)};
        }
        return false;
    };
    if (!value.is_object()) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope must be an object");
    }
    if (object_string(value, "format") != ENVELOPE_FORMAT) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope has an invalid format marker");
    }
    const auto* version = object_member(value, "version");
    if (version == nullptr || version->as_number() == nullptr) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope version must be a JSON integer");
    }
    uint32_t parsed_version = 0;
    const auto& lexeme = version->as_number()->lexeme;
    const auto [end, conversion_error] = std::from_chars(lexeme.data(), lexeme.data() + lexeme.size(), parsed_version);
    if (conversion_error != std::errc{} || end != lexeme.data() + lexeme.size()) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope version must be an unsigned integer");
    }
    if (parsed_version != ENVELOPE_VERSION) {
        return fail(ErrorCode::UNSUPPORTED_VERSION, "unsupported telemetry envelope major version");
    }
    if (!object_string(value, "source") || !object_string(value, "kind")) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope requires string source and kind fields");
    }
    const auto* source_version = object_member(value, "source_version");
    if (source_version == nullptr || source_version->as_number() == nullptr) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope source_version must be a JSON integer");
    }
    uint32_t parsed_source_version = 0;
    const auto& source_lexeme = source_version->as_number()->lexeme;
    const auto [source_end, source_error] =
        std::from_chars(source_lexeme.data(), source_lexeme.data() + source_lexeme.size(), parsed_source_version);
    if (source_error != std::errc{} || source_end != source_lexeme.data() + source_lexeme.size()) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry envelope source_version must be an unsigned integer");
    }
    for (const auto name : {"identity", "clock", "correlation", "payload"}) {
        const auto* member = object_member(value, name);
        if (member == nullptr || !member->is_object()) {
            return fail(ErrorCode::INVALID_ENVELOPE, std::string("telemetry envelope requires object field ") + name);
        }
    }
    const auto* identity = object_member(value, "identity");
    for (const auto name : {"boot_id", "pid", "tid"}) {
        const auto* member = object_member(*identity, name);
        if (member != nullptr && !decimal_u64(*member)) {
            return fail(ErrorCode::INVALID_ENVELOPE, std::string("telemetry identity field ") + name + " must be a decimal uint64 string");
        }
    }
    for (const auto name : {"node_id", "node"}) {
        const auto* member = object_member(*identity, name);
        if (member != nullptr && !member->is_string()) {
            return fail(ErrorCode::INVALID_ENVELOPE, std::string("telemetry identity field ") + name + " must be a string");
        }
    }
    if (const auto* cpu = object_member(*identity, "cpu"); cpu != nullptr) {
        const auto* number = cpu->as_number();
        uint32_t parsed_cpu = 0;
        if (number == nullptr) {
            return fail(ErrorCode::INVALID_ENVELOPE, "telemetry identity field cpu must be a uint32 number");
        }
        const auto [cpu_end, cpu_error] = std::from_chars(number->lexeme.data(), number->lexeme.data() + number->lexeme.size(), parsed_cpu);
        if (cpu_error != std::errc{} || cpu_end != number->lexeme.data() + number->lexeme.size()) {
            return fail(ErrorCode::INVALID_ENVELOPE, "telemetry identity field cpu must be a uint32 number");
        }
    }
    const auto* clock = object_member(value, "clock");
    for (const auto name : {"domain", "unit", "quality"}) {
        const auto* member = object_member(*clock, name);
        if (member != nullptr && !member->is_string()) {
            return fail(ErrorCode::INVALID_ENVELOPE, std::string("telemetry clock field ") + name + " must be a string");
        }
    }
    if (const auto* clock_value = object_member(*clock, "value"); clock_value != nullptr && !decimal_u64(*clock_value)) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry clock field value must be a decimal uint64 string");
    }
    if (const auto* offset = object_member(*clock, "realtime_offset_ns"); offset != nullptr && !decimal_i64(*offset)) {
        return fail(ErrorCode::INVALID_ENVELOPE, "telemetry clock field realtime_offset_ns must be a decimal int64 string");
    }
    return true;
}

auto make_envelope(std::string source, uint32_t source_version, std::string kind, Value::Object identity, Value::Object clock,
                   Value::Object correlation, Value::Object payload, Value::Object extensions) -> Value {
    Value::Object root = std::move(extensions);
    root.insert_or_assign("format", Value(ENVELOPE_FORMAT));
    root.insert_or_assign("version", Value::number(decimal_u64_string(ENVELOPE_VERSION)));
    root.insert_or_assign("source", Value(std::move(source)));
    root.insert_or_assign("source_version", Value::number(decimal_u64_string(source_version)));
    root.insert_or_assign("kind", Value(std::move(kind)));
    root.insert_or_assign("identity", Value(std::move(identity)));
    root.insert_or_assign("clock", Value(std::move(clock)));
    root.insert_or_assign("correlation", Value(std::move(correlation)));
    root.insert_or_assign("payload", Value(std::move(payload)));
    return Value(std::move(root));
}

auto crc32c(std::span<const std::byte> bytes) -> uint32_t {
    uint32_t crc = 0xffffffffU;
    for (const auto byte : bytes) {
        crc ^= std::to_integer<uint8_t>(byte);
        for (unsigned bit = 0; bit < 8; ++bit) {
            const uint32_t MASK = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0x82f63b78U & MASK);
        }
    }
    return ~crc;
}

auto crc32c(std::string_view bytes) -> uint32_t { return crc32c(std::as_bytes(std::span(bytes.data(), bytes.size()))); }

auto encode_container(const BinaryContainer& container, ContainerLimits limits) -> EncodeContainerResult {
    if (container.sections.size() > limits.max_sections || container.sections.size() > std::numeric_limits<uint32_t>::max()) {
        return {{}, {ErrorCode::SECTION_LIMIT, 0, "container has too many sections"}};
    }
    size_t header_bytes = 0;
    if (!checked_add(CONTAINER_HEADER_SIZE, container.sections.size() * SECTION_HEADER_SIZE, header_bytes)) {
        return {{}, {ErrorCode::CONTAINER_TOO_LARGE, 0, "container header length overflows"}};
    }
    size_t total_bytes = header_bytes;
    for (const auto& section : container.sections) {
        if (section.payload.size() > limits.max_section_bytes || !checked_add(total_bytes, section.payload.size(), total_bytes)) {
            return {{}, {ErrorCode::CONTAINER_TOO_LARGE, 0, "container section or total length exceeds its limit"}};
        }
    }
    if (total_bytes > limits.max_total_bytes || header_bytes > std::numeric_limits<uint32_t>::max()) {
        return {{}, {ErrorCode::CONTAINER_TOO_LARGE, 0, "container exceeds the configured byte limit"}};
    }

    std::vector<std::byte> output;
    output.reserve(total_bytes);
    for (const char byte : container.magic) output.push_back(static_cast<std::byte>(byte));
    append_le<uint16_t>(output, container.major);
    append_le<uint16_t>(output, container.minor);
    append_le<uint32_t>(output, static_cast<uint32_t>(header_bytes));
    append_le<uint64_t>(output, static_cast<uint64_t>(total_bytes));
    append_le<uint32_t>(output, static_cast<uint32_t>(container.sections.size()));
    append_le<uint32_t>(output, container.flags);
    append_le<uint32_t>(output, 0);
    append_le<uint32_t>(output, 0);
    for (const auto& section : container.sections) {
        append_le<uint32_t>(output, section.type);
        append_le<uint32_t>(output, section.flags);
        append_le<uint64_t>(output, static_cast<uint64_t>(section.payload.size()));
        append_le<uint32_t>(output, crc32c(section.payload));
        append_le<uint32_t>(output, 0);
    }
    const uint32_t HEADER_CRC = crc_with_zeroed_header_field(std::span<const std::byte>(output).first(header_bytes));
    for (size_t i = 0; i < sizeof(HEADER_CRC); ++i) {
        output[HEADER_CRC_OFFSET + i] = static_cast<std::byte>((HEADER_CRC >> (i * 8U)) & 0xffU);
    }
    for (const auto& section : container.sections) {
        output.insert(output.end(), section.payload.begin(), section.payload.end());
    }
    return {std::move(output), {}};
}

auto decode_container(std::span<const std::byte> input, std::optional<std::array<char, 8>> expected_magic, ContainerLimits limits)
    -> DecodeContainerResult {
    if (input.size() > limits.max_total_bytes) {
        return {{}, {ErrorCode::CONTAINER_TOO_LARGE, 0, "container exceeds the configured byte limit"}};
    }
    if (input.size() < CONTAINER_HEADER_SIZE) {
        return {{}, {ErrorCode::INVALID_CONTAINER, input.size(), "truncated container header"}};
    }
    BinaryContainer container;
    for (size_t i = 0; i < container.magic.size(); ++i) {
        container.magic[i] = static_cast<char>(std::to_integer<unsigned char>(input[i]));
    }
    if (expected_magic && container.magic != *expected_magic) {
        return {{}, {ErrorCode::INVALID_CONTAINER, 0, "container magic does not match"}};
    }
    container.major = read_le<uint16_t>(input, 8);
    container.minor = read_le<uint16_t>(input, 10);
    const uint32_t HEADER_BYTES = read_le<uint32_t>(input, 12);
    const uint64_t TOTAL_BYTES = read_le<uint64_t>(input, 16);
    const uint32_t SECTION_COUNT = read_le<uint32_t>(input, 24);
    container.flags = read_le<uint32_t>(input, 28);
    const uint32_t HEADER_CRC = read_le<uint32_t>(input, HEADER_CRC_OFFSET);
    size_t expected_header = 0;
    if (SECTION_COUNT > limits.max_sections ||
        !checked_add(CONTAINER_HEADER_SIZE, static_cast<size_t>(SECTION_COUNT) * SECTION_HEADER_SIZE, expected_header) ||
        HEADER_BYTES != expected_header || HEADER_BYTES > input.size() || TOTAL_BYTES != input.size()) {
        return {{}, {ErrorCode::INVALID_CONTAINER, 12, "invalid container header lengths or section count"}};
    }
    if (HEADER_CRC != crc_with_zeroed_header_field(input.first(HEADER_BYTES))) {
        return {{}, {ErrorCode::CHECKSUM_MISMATCH, HEADER_CRC_OFFSET, "container header checksum mismatch"}};
    }
    struct Descriptor {
        uint32_t type;
        uint32_t flags;
        size_t length;
        uint32_t crc;
    };
    std::vector<Descriptor> descriptors;
    descriptors.reserve(SECTION_COUNT);
    size_t payload_offset = HEADER_BYTES;
    for (size_t index = 0; index < SECTION_COUNT; ++index) {
        const size_t OFFSET = CONTAINER_HEADER_SIZE + index * SECTION_HEADER_SIZE;
        const uint64_t LENGTH = read_le<uint64_t>(input, OFFSET + 8);
        if (LENGTH > limits.max_section_bytes || LENGTH > std::numeric_limits<size_t>::max()) {
            return {{}, {ErrorCode::CONTAINER_TOO_LARGE, OFFSET + 8, "container section exceeds its byte limit"}};
        }
        size_t end = 0;
        if (!checked_add(payload_offset, static_cast<size_t>(LENGTH), end) || end > input.size()) {
            return {{}, {ErrorCode::INVALID_CONTAINER, OFFSET + 8, "container section length exceeds input"}};
        }
        descriptors.push_back({read_le<uint32_t>(input, OFFSET), read_le<uint32_t>(input, OFFSET + 4), static_cast<size_t>(LENGTH),
                               read_le<uint32_t>(input, OFFSET + 16)});
        payload_offset = end;
    }
    if (payload_offset != input.size()) {
        return {{}, {ErrorCode::INVALID_CONTAINER, payload_offset, "container has trailing or unclaimed bytes"}};
    }
    payload_offset = HEADER_BYTES;
    container.sections.reserve(SECTION_COUNT);
    for (const auto& descriptor : descriptors) {
        const auto payload = input.subspan(payload_offset, descriptor.length);
        if (crc32c(payload) != descriptor.crc) {
            return {{}, {ErrorCode::CHECKSUM_MISMATCH, payload_offset, "container section checksum mismatch"}};
        }
        container.sections.push_back(
            BinarySection{descriptor.type, descriptor.flags, std::vector<std::byte>(payload.begin(), payload.end())});
        payload_offset += descriptor.length;
    }
    return {std::move(container), {}};
}

}  // namespace wos::telemetry
