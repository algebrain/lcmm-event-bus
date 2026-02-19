# Repository Guidelines

## Project Structure & Module Organization

`src/` contains the core library code (primary namespace is `event-bus` in `src/event_bus.clj`).  
`test/` holds unit tests (e.g., `test/event_bus_test.clj`).  
`docs/` contains architecture notes, API docs, and behavior requirements.  
When you ask to create a plan for operations, the plan document must be placed in `.plans/`.  
When you ask to save an answer, it must be placed in `.answers/`.  
Both directories are included in `.gitignore` and should not be committed.  
Use `README.md` as the entry point, then follow links into the docs directories for deeper design details.

## Build, Test, and Development Commands

This repository is a Clojure library with no separate build or run scripts. The key workflow is testing:

- `clj -M:test`  
  Runs the full test suite via Kaocha as configured in `tests.edn`.
- `test.bat`  
  Windows helper that runs tests with the documentation reporter.

If you need a REPL workflow, start it with your preferred `clj` invocation and load the `event-bus` namespace.

## Coding Style & Naming Conventions

All files must be saved as UTF-8 without BOM. BOM is not allowed.  
Для файловых операций (чтение/запись) использовать Node.js вместо PowerShell, чтобы избежать проблем с кодировкой.  
Communication is preferred in Russian; code comments should be in English.  
Follow idiomatic Clojure formatting: 2-space indentation, align maps and bindings, and keep pure functions small.  
Namespace naming uses kebab-case (`event-bus`), while filenames use underscores (`event_bus.clj`).  
Keywords follow `:domain/name` or `:module/name` patterns (e.g., `:test/event`).

There is no formatter or linter configured in this repo; preserve existing style and avoid reformatting unrelated code.

## Behavior & Architecture Notes

The bus is in-process and built around event envelopes. Keep these rules intact:

- `publish` requires `:module` in opts.
- `:schema-registry` is required in `make-bus`; `publish` validates strictly and fails on missing/invalid schemas.
- `:schema-version` defaults to `"1.0"`.
- `CausationPath` is a vector of `[module event-type]` pairs with cycle detection.
- Logger failures must not break the critical path.
- In buffered mode, consumers run on virtual threads via the shared executor.

## Testing Guidelines

Start tests using `test.bat` in Windows.
Tests run through Kaocha.  
Test files live in `test/` and use `*-test` namespaces (e.g., `event-bus-test`).  
No explicit coverage target is defined; add tests for new behavior and edge cases, especially around causality and schema validation.

## Commit & Pull Request Guidelines

Commit history shows short, descriptive messages (e.g., "README fix", "significant changes in code and documentation").  
There is no strict convention, but keep commits concise and focused on a single change.

For pull requests, include:

- A brief summary of behavior changes.
- Test command(s) run and their results.
- Doc updates in `README.md`, `docs/` when behavior or API changes.

## Documentation Touchpoints

If you change public API or behavior, update `docs/BUS.md`, `docs/ARCH.md` (and English variants if present) and keep `README.md` in sync.
