from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .agent import diagnose
from .connectors import default_connectors
from .control import ControlStore
from .engine import MetadataDrivenIngestion
from .gold import GoldProductPublisher
from .metadata import MetadataRegistry
from .storage import LocalJsonSink
from .synthetic import generate

ROOT = Path(__file__).resolve().parents[2]


def build_engine(metadata_dir: Path, state_dir: Path, synthetic_profile: bool = False):
    return MetadataDrivenIngestion(
        MetadataRegistry(metadata_dir), default_connectors(), LocalJsonSink(),
        ControlStore(state_dir / "control.sqlite"),
        pii_key=(b"local-only-synthetic-key-rotate-in-production" if synthetic_profile else None),
        downstream=GoldProductPublisher(state_dir / "gold"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Metadata-driven lakehouse ingestion framework")
    sub = parser.add_subparsers(dest="command", required=True)
    generate_cmd = sub.add_parser("generate"); generate_cmd.add_argument("--output", type=Path, required=True)
    generate_cmd.add_argument("--patients", type=int, default=100); generate_cmd.add_argument("--days", type=int, default=7)
    generate_cmd.add_argument("--seed", type=int, default=42)
    run_cmd = sub.add_parser("run"); run_cmd.add_argument("--dataset", default="pmp_patient_services")
    run_cmd.add_argument("--metadata", type=Path, default=ROOT / "config/metadata")
    run_cmd.add_argument("--state-dir", type=Path, default=Path("build/lakehouse"))
    sample_cmd = sub.add_parser("sample-run"); sample_cmd.add_argument("--output", type=Path, default=Path("build/sample-run"))
    diagnose_cmd = sub.add_parser("diagnose"); diagnose_cmd.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "generate":
        generate(args.output, args.patients, args.days, args.seed)
        print(f"Synthetic CDC source created: {args.output / 'pmp_cdc.jsonl'}")
        return
    if args.command == "diagnose":
        report = diagnose(args.run_dir)
        (args.run_dir / "agent_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        return
    if args.command == "sample-run":
        output = args.output.resolve()
        input_dir = output / "input"
        generate(input_dir)
        os.environ["PMP_CDC_FIXTURE"] = str(input_dir / "pmp_cdc.jsonl")
        state_dir = output / "lakehouse"
        os.environ["LAKEHOUSE_ROOT"] = str(state_dir)
        result = build_engine(ROOT / "config/metadata", state_dir, synthetic_profile=True).execute("pmp_patient_services")
    else:
        state_dir = args.state_dir.resolve()
        os.environ["LAKEHOUSE_ROOT"] = str(state_dir)
        result = build_engine(args.metadata.resolve(), state_dir).execute(args.dataset)
    print(json.dumps(result.__dict__, indent=2))
    if result.status != "SUCCEEDED":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
