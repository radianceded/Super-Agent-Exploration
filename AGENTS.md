# Working on this runtime

- Read README.md and docs/decisions.md before changing orchestration semantics.
- Keep real events, model logs, authentication and local configuration out of Git. Use synthetic identities in tests and examples.
- Preserve source message identity, transactional routing, run-token fencing and output acknowledgement after delivery.
- Never equate model completion text with independently verified success.
- Keep adapters explicit about their actual transport; source labels alone are not platform integrations.
- Run `python -m unittest discover -v` and `python scripts/demo.py` for core changes. Run supervisor fault injection for process-lifecycle changes.
- The real Codex smoke test uses local login/quota and is separate from offline CI. Report which checks actually ran.
- Do not claim cloud execution, full-duplex steering, semantic supervision or multi-tenant isolation until implemented and verified.
