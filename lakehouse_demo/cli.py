from __future__ import annotations

import argparse
import json
from pathlib import Path

from .agent import diagnose
from .pipeline import run
from .synthetic import generate

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "pmp_patient_services.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic patient-services lakehouse demo")
    sub = parser.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate"); g.add_argument("--output", type=Path, required=True)
    g.add_argument("--patients", type=int, default=100); g.add_argument("--days", type=int, default=7); g.add_argument("--seed", type=int, default=42)
    r = sub.add_parser("run"); r.add_argument("--input", type=Path, required=True); r.add_argument("--output", type=Path, required=True)
    d = sub.add_parser("diagnose"); d.add_argument("--run-dir", type=Path, required=True)
    demo = sub.add_parser("demo"); demo.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "generate":
        generate(args.output, args.patients, args.days, args.seed)
        print(f"Synthetic source generated at {args.output}")
    elif args.command == "run":
        result = run(args.input, args.output, CONFIG)
        print(json.dumps(result, indent=2))
        if not result["quality"]["passed"] or not result["reconciliation"]["passed"]:
            raise SystemExit(2)
    elif args.command == "diagnose":
        report = diagnose(args.run_dir)
        (args.run_dir / "agent_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    else:
        input_dir, run_dir = args.output / "input", args.output / "run"
        generate(input_dir)
        result = run(input_dir, run_dir, CONFIG)
        report = diagnose(run_dir)
        (run_dir / "agent_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({**result, "agent": report}, indent=2))
        if not result["quality"]["passed"] or not result["reconciliation"]["passed"]:
            raise SystemExit(2)


if __name__ == "__main__":
    main()
