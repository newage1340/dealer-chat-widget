"""
Command-line entry point.

  python src/main.py                         run the development server (PORT, default 5001)
  python src/main.py validate-domain FILE    check a domain pack TOML and report every problem
  python src/main.py prompt SLUG [CHANNEL] [MESSAGE]
                                             print the exact prompt a turn would get (sms|web|voice)
  python src/main.py author-prompt "BUSINESS TYPE" [CONTEXT]
                                             print the prompt leader for writing a new domain pack by hand
  python src/main.py author-domain "BUSINESS TYPE" [CONTEXT]
                                             generate, validate and save a new pack with the configured LLM
"""

import os
import sys


def run_server() -> None:
    from server import create_app

    app = create_app()
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


def validate_domain(path: str) -> int:
    from domains import DomainPackError, load_pack

    try:
        pack = load_pack(path)
    except DomainPackError as e:
        print(f"INVALID {e}")
        return 1
    print(f"OK  {pack.id} ({pack.name}): {len(pack.catalog.schema.fields)} catalog fields, "
          f"{len(pack.profile)} profile questions, {len(pack.booking.intake)} intake fields, "
          f"{len(pack.widget.topics)} widget topics")
    return 0


def print_prompt(slug: str, channel_name: str = "sms", message: str = "Hi, what do you have?") -> int:
    from config import Config
    from core import Channel, Contact, ContactId
    from domains import DomainRegistry
    from pipeline.services import tenant_now
    from prompts import PromptComposer, PromptInputs
    from tenants import TenantDirectory

    cfg = Config.from_env()
    registry = DomainRegistry.from_directory()
    directory = TenantDirectory.from_directory(cfg.tenants_dir, registry)
    record = directory.by_slug(slug)
    if record is None:
        print(f"no tenant with slug '{slug}'", file=sys.stderr)
        return 1
    channel = Channel(channel_name)
    composer = PromptComposer()
    now = tenant_now(record.tenant)
    inputs = PromptInputs(
        tenant=record.tenant, pack=registry.get(record.tenant.domain), items=record.items,
        contact=Contact(id=ContactId("preview"), tenant_id=record.tenant.id, first_name="Alex", phone="+13175550199"),
        history=(), message=message, channel=channel, now=now,
        caller_phone="+13175550199" if channel is Channel.VOICE else "",
    )
    messages = composer.voice_messages(inputs) if channel is Channel.VOICE else composer.chat_messages(inputs)
    for m in messages:
        print(f"##### {m.role}\n{m.content}\n")
    return 0


def author_prompt(business_type: str, context: str = "") -> int:
    from domains.authoring import authoring_prompt

    print(authoring_prompt(business_type, context))
    return 0


def author_domain(business_type: str, context: str = "") -> int:
    from config import Config
    from domains import DomainPackError
    from domains.authoring import generate_pack, save_pack
    from providers import ListUsageSink, PriceTable
    from server import build_llm

    llm = build_llm(Config.from_env().llm, ListUsageSink(), PriceTable())
    try:
        text, pack = generate_pack(llm, business_type, context)
    except DomainPackError as e:
        print(f"Generated pack failed validation:\n{e}", file=sys.stderr)
        return 1
    try:
        path = save_pack(text, pack)
    except FileExistsError as e:
        print(e, file=sys.stderr)
        return 1
    print(f"Saved {path}. Review it, then point a tenant at domain \"{pack.id}\".")
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        run_server()
        return 0
    command, args = argv[0], argv[1:]
    if command == "validate-domain" and len(args) == 1:
        return validate_domain(args[0])
    if command == "prompt" and 1 <= len(args) <= 3:
        return print_prompt(*args)
    if command == "author-prompt" and 1 <= len(args) <= 2:
        return author_prompt(*args)
    if command == "author-domain" and 1 <= len(args) <= 2:
        return author_domain(*args)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
