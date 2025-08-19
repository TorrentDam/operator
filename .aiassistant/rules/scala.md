---
apply: always
patterns: .scala
---

# Scala coding style

The instructions below must be followed whenever Scala code is written.
If existing examples from docs are used, they must be re-written using this coding style.

## Use Scala 3 syntax

- wildcard imports must use `*` (ex: `import io.sample.*`)
- prefer fully-qualified imports
- use braceless syntax
- use `given` and `using` instead of `implicit`