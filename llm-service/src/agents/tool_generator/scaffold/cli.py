"""
Command line entry point for the deterministic connector scaffolder.

    python -m agents.tool_generator.scaffold.cli openapi.json --out ./my-connector

Reads an OpenAPI 3.x or Swagger 2.0 document from a file (or stdin via ``-``),
converts it to a ConnectorSpec, renders the connector, and writes the artifacts.
No network access, no API key, no model call.

VERSION: 1.0.0
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

from .openapi_to_spec import ConversionReport, OpenAPIConversionError, openapi_to_connector_spec

logger = logging.getLogger(__name__)

_CATEGORIES = ("api_saas", "relational_db", "document_db", "cloud_storage", "streaming", "data_warehouse", "wide_column_db")


def load_document(path: str) -> Dict[str, Any]:
    """Parse a spec file as JSON, falling back to YAML.

    JSON is tried first because every JSON document is also valid YAML but not
    the other way round, and the JSON parser gives better error positions.
    """
    if path == "-":
        raw = sys.stdin.read()
        origin = "stdin"
    else:
        if not os.path.isfile(path):
            raise OpenAPIConversionError(f"No such file: {path}")
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
        origin = path

    if not raw.strip():
        raise OpenAPIConversionError(f"{origin} is empty")

    try:
        return json.loads(raw)
    except ValueError as json_error:
        try:
            import yaml  # Optional: most published specs are YAML.
        except ImportError:
            raise OpenAPIConversionError(
                f"{origin} is not valid JSON ({json_error}), and PyYAML is not installed "
                "so it cannot be read as YAML. Install PyYAML or convert the file to JSON."
            )
        try:
            parsed = yaml.safe_load(raw)
        except Exception as yaml_error:
            raise OpenAPIConversionError(f"{origin} parses as neither JSON nor YAML: {yaml_error}")
        if not isinstance(parsed, dict):
            raise OpenAPIConversionError(f"{origin} does not contain a mapping at the document root")
        return parsed


def _import_builder():
    """Import ConnectorBuilder, tolerating both package and flat layouts.

    Mirrors the dual-path imports the rest of tool_generator uses so the CLI
    runs the same way inside the container and from a source checkout.
    """
    try:
        from ..generator.builder import ConnectorBuilder  # type: ignore
        return ConnectorBuilder
    except ImportError:
        from generator.builder import ConnectorBuilder  # type: ignore
        return ConnectorBuilder


def write_artifacts(generated: Any, spec: Dict[str, Any], out_dir: str) -> List[str]:
    """Persist the rendered connector.

    Deliberately does NOT call ``GeneratedConnector.save_to_directory``: that
    method also fetches a logo over the network, which would make the
    scaffolder neither offline nor reproducible. The two refusal guards it
    applies are reproduced here so an invalid or empty build still cannot be
    written to disk.
    """
    if not getattr(generated, "is_valid", False):
        raise OpenAPIConversionError(
            "Refusing to write an invalid connector: " + "; ".join(generated.validation_errors)
        )
    if not (generated.code or "").strip():
        raise OpenAPIConversionError("Refusing to write an empty connector body")

    os.makedirs(out_dir, exist_ok=True)
    files = {
        "connector.py": generated.code,
        "metadata.json": generated.metadata,
        "requirements.txt": generated.requirements,
        # The spec is an artifact in its own right: edit it and re-render
        # without going back to the OpenAPI document.
        "spec.json": json.dumps(spec, indent=2, sort_keys=True) + "\n",
    }
    if generated.dockerfile:
        files["Dockerfile"] = generated.dockerfile

    written = []
    for filename, content in files.items():
        target = os.path.join(out_dir, filename)
        with open(target, "w", encoding="utf-8") as fh:
            fh.write(content)
        written.append(target)
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rsync-scaffold",
        description="Generate an MCP connector from an OpenAPI 3.x or Swagger 2.0 document. Deterministic and offline: no LLM, no API key, no account.",
    )
    parser.add_argument("spec", help="Path to the OpenAPI/Swagger document, or '-' to read it from stdin")
    parser.add_argument("--out", "-o", help="Directory to write the connector into (omit for a dry run)")
    parser.add_argument("--name", help="Connector identifier (default: slug of info.title)")
    parser.add_argument("--display-name", help="Human-readable name (default: info.title)")
    parser.add_argument("--base-url", help="Override the API base URL the document declares")
    parser.add_argument("--category", default="api_saas", choices=_CATEGORIES, help="Connector category (default: api_saas)")
    parser.add_argument("--max-resources", type=int, help="Keep at most this many resources")
    parser.add_argument("--with-destination", action="store_true", help="Advertise write support as well as read")
    parser.add_argument("--print-spec", action="store_true", help="Print the generated ConnectorSpec as JSON")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output directory")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show library log output")
    return parser


def run(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.ERROR,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        document = load_document(args.spec)
        report: ConversionReport = openapi_to_connector_spec(
            document,
            name=args.name,
            display_name=args.display_name,
            category=args.category,
            base_url=args.base_url,
            supports_destination=args.with_destination,
            max_resources=args.max_resources,
        )
    except OpenAPIConversionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    spec = report.spec
    print(f"{spec['display_name']} ({spec['name']})")
    print(f"  base URL:  {spec['base_url'] or '(none declared)'}")
    print(f"  auth:      {spec['auth']['type']}")
    print(f"  resources: {report.resource_count}")
    for resource in spec["resources"]:
        verbs = "".join(
            flag for flag, enabled in (
                ("C", resource.get("supports_create")),
                ("R", True),
                ("U", resource.get("supports_update")),
                ("D", resource.get("supports_delete")),
            ) if enabled
        )
        print(f"    - {resource['name']:<24} {resource['endpoint']}  [{verbs}]")
    if report.skipped_paths:
        print(f"  skipped:   {len(report.skipped_paths)} path(s); re-run with -v to list them")
        for skipped in report.skipped_paths:
            logger.info("skipped %s", skipped)
    for note in report.notes:
        print(f"  note:      {note}")

    if args.print_spec:
        print(json.dumps(spec, indent=2, sort_keys=True))

    if not args.out:
        print("\nDry run: pass --out DIR to render the connector.")
        return 0

    if os.path.exists(args.out) and os.listdir(args.out) and not args.force:
        print(f"error: {args.out} exists and is not empty; pass --force to overwrite", file=sys.stderr)
        return 2

    try:
        builder_cls = _import_builder()
        generated = builder_cls().build_from_dict(spec)
    except Exception as exc:
        print(f"error: rendering failed: {exc}", file=sys.stderr)
        return 1

    for warning in generated.validation_warnings:
        print(f"  warning:   {warning}")

    try:
        written = write_artifacts(generated, spec, args.out)
    except OpenAPIConversionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"\nWrote {len(written)} files to {args.out}:")
    for path in written:
        print(f"  {os.path.basename(path)}")
    print("\nNext: set the connector's credential env var, then `docker build` the directory.")
    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
