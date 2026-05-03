# WOS Logging And Journal Conventions

Apply these to all kernel code.

- Kernel logging must use the journal-backed logging API.
- New module code should define a typed logger, for example:
  `using log = ker::mod::dbg::logger<"wki">;`
- Use `log::trace()`, `log::debug()`, `log::info()`, `log::warn()`, `log::error()`, etc. instead of manually prefixing messages with `[TAG]`.
- Existing `ker::mod::dbg::log()` call sites may remain only as compatibility during migration; new code should use a module logger.
- Do not write routine logs directly to serial. Direct serial writes are allowed only in panic paths, very early boot before the journal is initialized, the serial driver itself, and emergency dump paths where allocation or scheduler interaction is unsafe.
- If a subsystem has disabled or commented-out debug logging, migrate it to `trace` or `debug` journal calls instead of leaving ad hoc serial/debug code.
