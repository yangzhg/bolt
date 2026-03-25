#include "bolt/tool/boltfs/BoltFs.h"

#include <folly/init/Init.h>

#if defined(BOLTFS_HAS_READLINE)
#include <readline/history.h>
#include <readline/readline.h>
#endif

#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

using namespace bytedance::bolt::tool::boltfs;

namespace {

BoltFs* gBoltFsCompletion = nullptr;

ClientMode parseClientMode() {
  if (const char* mode = std::getenv("BOLTFS_CLIENT_MODE")) {
    const std::string_view value{mode};
    if (value == "agent") {
      return ClientMode::kAgent;
    }
    if (value == "human") {
      return ClientMode::kHuman;
    }
    if (value != "auto") {
      throw std::runtime_error(
          "Unsupported BOLTFS_CLIENT_MODE. Use auto, agent, or human.");
    }
  }

  return ::isatty(STDOUT_FILENO) ? ClientMode::kHuman : ClientMode::kAgent;
}

std::string readInteractiveLine() {
#if defined(BOLTFS_HAS_READLINE)
  if (::isatty(STDIN_FILENO) && ::isatty(STDOUT_FILENO)) {
    std::unique_ptr<char, decltype(&std::free)> line(
        readline("boltfs:/> "), &std::free);
    if (!line) {
      return {};
    }
    if (*line) {
      add_history(line.get());
    }
    return std::string(line.get());
  }
#endif

  std::cout << "boltfs:/> " << std::flush;
  std::string line;
  if (!std::getline(std::cin, line)) {
    return {};
  }
  return line;
}

#if defined(BOLTFS_HAS_READLINE)
char** completionHook(const char* text, int start, int end) {
  (void)end;
  if (gBoltFsCompletion == nullptr) {
    return nullptr;
  }

  std::vector<std::string> candidates;
  if (start == 0) {
    candidates = gBoltFsCompletion->completeCommand(text);
  } else {
    const std::string line(rl_line_buffer, start);
    size_t firstNonSpace = line.find_first_not_of(' ');
    if (firstNonSpace != std::string::npos) {
      const auto firstSpace = line.find(' ', firstNonSpace);
      const auto command = line.substr(
          firstNonSpace,
          firstSpace == std::string::npos ? std::string::npos
                                          : firstSpace - firstNonSpace);
      if (command == "ls" || command == "schema" || command == "sample" ||
          command == "cat") {
        candidates = gBoltFsCompletion->completePath(text);
      }
    }
  }

  if (candidates.empty()) {
    return nullptr;
  }

  std::vector<char*> raw;
  raw.reserve(candidates.size() + 1);
  for (const auto& candidate : candidates) {
    raw.push_back(::strdup(candidate.c_str()));
  }
  raw.push_back(nullptr);

  auto** matches = static_cast<char**>(std::malloc(sizeof(char*) * raw.size()));
  for (size_t i = 0; i < raw.size(); ++i) {
    matches[i] = raw[i];
  }
  return matches;
}
#endif

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  BoltFs boltfs(parseClientMode());
#if defined(BOLTFS_HAS_READLINE)
  gBoltFsCompletion = &boltfs;
  rl_attempted_completion_function = completionHook;
#endif
  if (argc > 1) {
    std::string command;
    for (int i = 1; i < argc; ++i) {
      if (!command.empty()) {
        command.push_back(' ');
      }
      command += argv[i];
    }

    try {
      std::cout << boltfs.execute(command) << std::endl;
      std::cout.flush();
      std::cerr.flush();
      std::_Exit(0);
    } catch (const std::exception& e) {
      std::fprintf(stderr, "BoltFS error: %s\n", e.what());
      std::fflush(stdout);
      std::fflush(stderr);
      std::_Exit(1);
    } catch (...) {
      std::fprintf(stderr, "BoltFS error: unknown exception\n");
      std::fflush(stdout);
      std::fflush(stderr);
      std::_Exit(1);
    }
  }

  while (true) {
    const auto line = readInteractiveLine();
    if (!std::cin.good() && line.empty()) {
      break;
    }
    if (line.empty()) {
      if (std::cin.eof()) {
        break;
      }
      continue;
    }

    try {
      const auto output = boltfs.execute(line);
      if (output == "exit") {
        std::cout.flush();
        std::cerr.flush();
        std::_Exit(0);
        break;
      }
      std::cout << output << std::endl;
    } catch (const std::exception& e) {
      std::cout << "BoltFS error: " << e.what() << std::endl;
    }
  }
  std::cout.flush();
  std::cerr.flush();
  std::_Exit(0);
}
