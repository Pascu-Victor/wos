#include "testd.hpp"

TESTD_RUN(test_journal_device_userspace_record) {
    int const FD = open("/dev/journal", O_RDWR);
    if (FD < 0) {
        fail("journal_device_open", "open /dev/journal failed");
        return;
    }

    std::array<char, 64> token{};
    (void)testd_format_to_array(token, "testd-journal-%d", getpid());
    size_t const TOKEN_LEN = std::strlen(token.data());
    ssize_t const WRITTEN = write(FD, token.data(), TOKEN_LEN);
    if (WRITTEN < 0 || std::cmp_not_equal(WRITTEN, TOKEN_LEN)) {
        close(FD);
        fail("journal_device_write", "write to /dev/journal failed or was short");
        return;
    }
    TESTD_PASS("journal_device_write");

    bool found = false;
    std::array<ker::abi::sys_log::JournalRecord, JOURNAL_SCAN_BATCH> records{};
    for (size_t batch = 0; batch < JOURNAL_SCAN_BATCHES && !found; ++batch) {
        ssize_t const N =
            read_once_timeout(FD, records.data(), records.size() * sizeof(ker::abi::sys_log::JournalRecord), REMOTE_IPC_TIMEOUT_MS);
        if (N < 0) {
            close(FD);
            fail("journal_device_read", "read from /dev/journal failed");
            return;
        }
        if (N == 0) {
            break;
        }
        if ((N % static_cast<ssize_t>(sizeof(ker::abi::sys_log::JournalRecord))) != 0) {
            close(FD);
            fail("journal_record_size", "journal read returned partial record");
            return;
        }

        size_t const COUNT = static_cast<size_t>(N) / sizeof(ker::abi::sys_log::JournalRecord);
        for (size_t i = 0; i < COUNT; ++i) {
            const auto& rec = records.at(i);
            constexpr std::string_view USERSPACE_MODULE = "userspace";
            bool module_matches = true;
            for (size_t pos = 0; pos < USERSPACE_MODULE.size(); ++pos) {
                module_matches = module_matches && rec.module[pos] == USERSPACE_MODULE.at(pos);
            }
            module_matches = module_matches && rec.module[USERSPACE_MODULE.size()] == '\0';
            if (!module_matches) {
                continue;
            }
            bool message_matches = rec.message_len == TOKEN_LEN;
            for (size_t pos = 0; pos < TOKEN_LEN && message_matches; ++pos) {
                message_matches = rec.message[pos] == token.at(pos);
            }
            if (!message_matches) {
                continue;
            }
            if (rec.magic != ker::abi::sys_log::JOURNAL_RECORD_MAGIC || rec.version != ker::abi::sys_log::JOURNAL_RECORD_VERSION ||
                rec.header_size != sizeof(ker::abi::sys_log::JournalRecord) - ker::abi::sys_log::JOURNAL_MESSAGE_MAX ||
                rec.level != static_cast<uint8_t>(ker::abi::sys_log::sys_log_level::INFO)) {
                close(FD);
                fail("journal_record_abi", "userspace journal record had invalid ABI fields");
                return;
            }
            found = true;
            break;
        }
    }
    close(FD);

    if (!found) {
        fail("journal_device_find_record", "userspace journal record was not found");
        return;
    }
    TESTD_PASS("journal_device_userspace_record");
}
TESTD_RUN_END(test_journal_device_userspace_record)
