#define PACKAGE 1
#define PACKAGE_VERSION 1
#include <bfd.h>
#include <bfdlink.h>
#include <fcntl.h>
#include <oneapi/tbb.h>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/parallel_for.h>
#include <stdlib.h>
#include <unistd.h>

#include <execution>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
// Declare thread-local storage for the bfd object
thread_local bfd* abfd = nullptr;

namespace fs = std::filesystem;

#include <cxxabi.h>

std::string demangle(const std::string& mangledName) {
    int status = 0;
    char* demangled = abi::__cxa_demangle(mangledName.c_str(), nullptr, nullptr, &status);
    std::string result = (status == 0) ? demangled : mangledName;
    free(demangled);
    return result;
}

std::string getFunctionNameFromAddress(const std::string& address, const std::string& executable) {
    // Save the current stderr file descriptor
    int stderr_fd = dup(STDERR_FILENO);
    // Open /dev/null and redirect stderr to it
    int dev_null_fd = open("/dev/null", O_WRONLY);
    dup2(dev_null_fd, STDERR_FILENO);
    close(dev_null_fd);

    bfd* abfd;
    char** matching;
    abfd = bfd_openr(executable.c_str(), nullptr);
    if (!abfd) {
        // Restore stderr
        dup2(stderr_fd, STDERR_FILENO);
        close(stderr_fd);
        return "";
    }

    if (!bfd_check_format_matches(abfd, bfd_object, &matching)) {
        bfd_close(abfd);
        // Restore stderr
        dup2(stderr_fd, STDERR_FILENO);
        close(stderr_fd);
        return "";
    }
    bfd_vma addr = std::stoul(address, nullptr, 16);
    asection* section = bfd_get_section_by_name(abfd, ".text");
    if (!section) {
        bfd_close(abfd);
        // Restore stderr
        dup2(stderr_fd, STDERR_FILENO);
        close(stderr_fd);
        return "";
    }

    const char* filename;
    const char* functionname;
    unsigned int line;
    if (bfd_find_nearest_line(abfd, section, nullptr, addr - section->vma, &filename, &functionname, &line)) {
        bfd_close(abfd);
        // Restore stderr
        dup2(stderr_fd, STDERR_FILENO);
        close(stderr_fd);
        return address + "[" + (functionname ? demangle(functionname) : "unknown") + "](" + fs::relative(filename).string() + ":" +
               std::to_string(line) + ")";
    }

    bfd_close(abfd);
    // Restore stderr
    dup2(stderr_fd, STDERR_FILENO);
    close(stderr_fd);
    return "";
}

void processLogFile(const std::string& logFile, const std::string& executable) {
    std::ifstream inFile(logFile);
    std::string tempLogFile = std::filesystem::path(logFile).stem().string() + ".modified.log";
    std::ofstream outFile(tempLogFile);
    std::vector<std::string> lines;
    std::string line;

    while (std::getline(inFile, line)) {
        lines.push_back(line);
    }
    inFile.close();

    std::regex addressRegex("(0xffff[0-9a-fA-F]{12})");
    for (size_t i = 0; i < lines.size(); ++i) {
        std::string& line = lines[i];
        std::string resultString;
        std::smatch match;
        std::string::const_iterator searchStart(line.cbegin());
        while (std::regex_search(searchStart, line.cend(), match, addressRegex)) {
            std::string address = match.str();
            std::string sourceLocation = getFunctionNameFromAddress(address, executable);
            resultString += match.prefix().str() + sourceLocation;
            searchStart = match.suffix().first;
        }
        resultString += std::string(searchStart, line.cend());
        line = resultString;
    }

    for (const auto& modifiedLine : lines) {
        outFile << modifiedLine << std::endl;
    }

    std::cout << "Modified log saved to " << tempLogFile << std::endl;
}

int main() {
    int fd = open("/dev/null", O_WRONLY);
    dup2(fd, STDERR_FILENO);

    std::string executable = "./bin/wos";
    std::string logPattern = "qemu.[^(modified)]*.log";

    for (const auto& entry : fs::directory_iterator(".")) {
        if (fs::is_regular_file(entry) && std::regex_match(entry.path().filename().string(), std::regex(logPattern))) {
            processLogFile(entry.path().string(), executable);
        }
    }

    return 0;
}
