import sys
import argparse
import time
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_EF_XLSX, STAGE2_OUTPUT_DIR


def _print_step(title: str) -> None:
    print(f"\n=== {title} ===", flush=True)


def _safe_run(func, name: str) -> None:
    try:
        _print_step(f"Running: {name}")
        start = time.time()
        func()
        elapsed = time.time() - start
        print(f"Finished: {name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        # Continue with the rest of the pipeline even if this step fails
        print(f"[WARN] Step failed: {name} -> {exc}", flush=True)


def main() -> None:
    # Parse optional CLI arguments for flexibility
    ap = argparse.ArgumentParser(description="Run full mapping pipeline with optional date window and input workbook override.")
    ap.add_argument("--start", help="Start date (YYYY-MM-DD)", default="2025-01-01")
    g = ap.add_mutually_exclusive_group(required=False)
    # Default: 12-month window (can be overridden via --months or --end)
    g.add_argument("--months", type=int, help="Number of months to include starting at --start (e.g., 12)", default=12)
    g.add_argument("--end", help="End date (YYYY-MM-DD)")
    ap.add_argument("--input", help="Explicit input workbook for main_mapping (absolute path)")
    ap.add_argument(
        "--enable-cat1-all-together-contains",
        action="store_true",
        help="Enable Cat1 'All together contains' fallback matching (disabled by default due to false positives).",
    )
    ap.add_argument(
        "--run-forecasting",
        action="store_true",
        help="If set: run Forecasting step (can be slow). Default: off.",
    )
    ap.add_argument(
        "--skip-decarb-scenarios",
        action="store_true",
        help="If set: skip Decarbonization_Scenarios user scenarios. Default: run them.",
    )
    args, _unknown = ap.parse_known_args()
    # Prevent submodules using argparse from seeing these args
    try:
        sys.argv = [sys.argv[0]]
    except Exception:
        pass

    # Ensure working directory is the script directory
    script_dir = Path(__file__).resolve().parent
    try:
        Path.chdir(script_dir)  # type: ignore[attr-defined]
    except Exception:
        # Fallback for Python versions without Path.chdir
        import os
        os.chdir(str(script_dir))

    print("Orchestrator started (Run_Everything.py).", flush=True)
    print(f"Working directory: {Path.cwd()}", flush=True)

    # Feature flag: Cat1 All together contains
    # Default OFF. Can be enabled via CLI or environment variable.
    if getattr(args, "enable_cat1_all_together_contains", False):
        os.environ["ENABLE_CAT1_ALL_TOGETHER_CONTAINS"] = "1"
        print("[info] Enabled: ENABLE_CAT1_ALL_TOGETHER_CONTAINS=1", flush=True)

    # 1) End-to-end mapping pipeline
    try:
        import main_mapping  # type: ignore

        # If user provided an explicit input workbook, override the module constant
        try:
            if args.input:
                setattr(main_mapping, "INPUT_WORKBOOK_NAME", str(args.input))
                print(f"[info] Overrode main_mapping.INPUT_WORKBOOK_NAME -> {args.input}", flush=True)
        except Exception:
            pass

        # Prefer calling the function directly to avoid nested Python processes
        step_name = "main_mapping.process_all_sheets"
        _safe_run(getattr(main_mapping, "process_all_sheets"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run main_mapping: {exc}", flush=True)

    # 2) Append sources into mapped workbook
    try:
        import append_sources_to_mapped as append_src  # type: ignore

        step_name = "append_sources_to_mapped.main"
        _safe_run(getattr(append_src, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run append_sources_to_mapped: {exc}", flush=True)

    # 3) Apply double-counting rules (booklets) on merged workbook
    try:
        import double_countin_booklets as dc  # type: ignore

        step_name = "double_countin_booklets.main"
        _safe_run(getattr(dc, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run double_countin_booklets: {exc}", flush=True)

    # 4) Create FERA consolidated sheets (Scope 3 Cat 3 Fuel/Electricity)
    try:
        import fera_mapping as fera  # type: ignore

        step_name = "fera_mapping.main"
        _safe_run(getattr(fera, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run fera_mapping: {exc}", flush=True)

    # 4b) Post-process FERA Fuel (overwrite Klarakarbon co2e into primary column)
    try:
        import fera_mapping_fuel as fera_fuel  # type: ignore

        step_name = "fera_mapping_fuel.main"
        _safe_run(getattr(fera_fuel, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run fera_mapping_fuel: {exc}", flush=True)

    # 5) Aggregate company totals (requested to be included)
    try:
        import aggregate_company_totals as agg  # type: ignore

        step_name = "aggregate_company_totals.main"
        _safe_run(getattr(agg, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run aggregate_company_totals: {exc}", flush=True)

    # 6) Reorganize by GHGP category
    try:
        import reorganize_by_ghgp_category as ghgp  # type: ignore

        step_name = "reorganize_by_ghgp_category.main"
        _safe_run(getattr(ghgp, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run reorganize_by_ghgp_category: {exc}", flush=True)

    # 7) Final cleaning on GHGP workbook (drop columns per sheet)
    try:
        import final_cleaning_ghgp as fc  # type: ignore

        step_name = "final_cleaning_ghgp.main"
        _safe_run(getattr(fc, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run final_cleaning_ghgp: {exc}", flush=True)

    # 7b) Post-fix on GHGP clean: overlay Klarakarbon co2e (t).1 -> co2e (t) for S3 Cat 3 FERA
    try:
        import post_fix_fera_kbk as pf  # type: ignore

        step_name = "post_fix_fera_kbk.main"
        _safe_run(getattr(pf, "main"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not import or run post_fix_fera_kbk: {exc}", flush=True)
    # 8) Produce windowed workbook per provided date window (defaults: 2025-01-01 + 12 months)
    window_path = None
    try:
        from filter_run_output_by_period import filter_workbook  # type: ignore

        step_name = "filter_run_output_by_period.filter_workbook"
        _print_step(f"Running: {step_name}")
        start = time.time()
        window_path = filter_workbook(args.start, args.end, args.months, None)
        elapsed = time.time() - start
        print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not run window filter: {exc}", flush=True)

    # 8a) Post-fix: Klarakarbon Cat1 missing ef_id + add harmonised dummy_ef_id (in the window workbook)
    # Must run AFTER window filter and BEFORE downstream consumers (totals tables / forecasting / decarb).
    try:
        # If window_path wasn't returned, fall back to newest window workbook under output/
        if window_path is None:
            try:
                out_dir = STAGE2_OUTPUT_DIR
                candidates = sorted(out_dir.glob("mapped_results_window_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
                window_path = candidates[0] if candidates else None
            except Exception:
                window_path = None

        if window_path is not None:
            import fix_klarakarbon_dummy_ef_id_cat1 as kbk_dummy  # type: ignore

            step_name = "fix_klarakarbon_dummy_ef_id_cat1.apply_fix (in-place window)"
            _print_step(f"Running: {step_name}")
            start = time.time()
            try:
                n_efid, n_dummy = kbk_dummy.apply_fix(
                    Path(window_path),
                    Path(window_path),
                    sheet_name="S3 Cat 1 Purchased G&S",
                    ef_workbook=STAGE2_EF_XLSX,
                )
                elapsed = time.time() - start
                print(f"Filled ef_id: {n_efid}", flush=True)
                print(f"Set dummy_ef_id: {n_dummy}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
            except PermissionError as e:
                fallback = Path(window_path).with_name(f"{Path(window_path).stem}_KLARAKARBON_DUMMY_EFID_FIX{Path(window_path).suffix}")
                n_efid, n_dummy = kbk_dummy.apply_fix(
                    Path(window_path),
                    fallback,
                    sheet_name="S3 Cat 1 Purchased G&S",
                    ef_workbook=STAGE2_EF_XLSX,
                )
                window_path = fallback
                elapsed = time.time() - start
                print(f"[WARN] Could not overwrite (file locked): {e}", flush=True)
                print(f"Filled ef_id: {n_efid}", flush=True)
                print(f"Set dummy_ef_id: {n_dummy}", flush=True)
                print(f"Wrote fallback: {fallback}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not apply Klarakarbon dummy_ef_id fix: {exc}", flush=True)

    # 8b) Post-fix: Velox Cat 6 Business Travel co2e(t) = CO2e(kg)/1000 in the window workbook
    try:
        # If window_path wasn't returned, fall back to newest window workbook under output/
        if window_path is None:
            try:
                out_dir = STAGE2_OUTPUT_DIR
                candidates = sorted(out_dir.glob("mapped_results_window_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
                window_path = candidates[0] if candidates else None
            except Exception:
                window_path = None

        if window_path is not None:
            import fix_velox_cat6_co2e_t as velox_fix  # type: ignore

            step_name = "fix_velox_cat6_co2e_t.apply_fix (in-place window)"
            _print_step(f"Running: {step_name}")
            start = time.time()
            n_updated = velox_fix.apply_fix(Path(window_path), Path(window_path), sheet_name="S3 Cat 6 Business Travel")
            elapsed = time.time() - start
            print(f"Updated rows: {n_updated}", flush=True)
            print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not apply Velox Cat6 window fix: {exc}", flush=True)

    # 8c) Post-fix: Velox Purchased G&S (Services Spend rows) → co2e(t)=Spend_Euro*ef_value
    # Must run AFTER window filter and BEFORE rebuilding window totals tables.
    try:
        if window_path is not None:
            import fix_velox_cat1_services_spend_in_purchased_gs as velox_cat1_fix  # type: ignore

            step_name = "fix_velox_cat1_services_spend_in_purchased_gs.apply_fix (in-place window)"
            _print_step(f"Running: {step_name}")
            start = time.time()
            try:
                n_updated = velox_cat1_fix.apply_fix(
                    Path(window_path),
                    Path(window_path),
                    sheet_name="S3 Cat 1 Purchased G&S",
                    company="Velox",
                    sheet_booklets_value="Scope 3 Services Spend",
                )
                elapsed = time.time() - start
                print(f"Updated rows: {n_updated}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
            except PermissionError as e:
                fallback = Path(window_path).with_name(f"{Path(window_path).stem}_VELOX_CAT1_SERVICES_FIX{Path(window_path).suffix}")
                n_updated = velox_cat1_fix.apply_fix(
                    Path(window_path),
                    fallback,
                    sheet_name="S3 Cat 1 Purchased G&S",
                    company="Velox",
                    sheet_booklets_value="Scope 3 Services Spend",
                )
                window_path = fallback
                elapsed = time.time() - start
                print(f"[WARN] Could not overwrite (file locked): {e}", flush=True)
                print(f"Updated rows: {n_updated}", flush=True)
                print(f"Wrote fallback: {fallback}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not apply Velox Cat1 Services window fix: {exc}", flush=True)

    # 8d) Post-fix: BIMMS Purchased G&S (Services Spend rows) → backfill missing ef_value/ef_unit by ef_id
    # This is a safety net for manual 'boq_exact' rows; should not overwrite existing ef_value.
    try:
        if window_path is not None:
            import fix_bimms_missing_ef_value_in_purchased_gs as bimms_fix  # type: ignore

            ef_path = STAGE2_EF_XLSX
            step_name = "fix_bimms_missing_ef_value_in_purchased_gs.apply_fix (in-place window)"
            _print_step(f"Running: {step_name}")
            start = time.time()
            try:
                n_updated = bimms_fix.apply_fix(
                    Path(window_path),
                    Path(window_path),
                    ef_workbook=ef_path,
                    sheet_name="S3 Cat 1 Purchased G&S",
                    company="BIMMS",
                    sheet_booklets_value="Scope 3 Cat 1 Services Spend",
                )
                elapsed = time.time() - start
                print(f"Updated rows: {n_updated}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
            except PermissionError as e:
                fallback = Path(window_path).with_name(f"{Path(window_path).stem}_BIMMS_EF_BACKFILL{Path(window_path).suffix}")
                n_updated = bimms_fix.apply_fix(
                    Path(window_path),
                    fallback,
                    ef_workbook=ef_path,
                    sheet_name="S3 Cat 1 Purchased G&S",
                    company="BIMMS",
                    sheet_booklets_value="Scope 3 Cat 1 Services Spend",
                )
                window_path = fallback
                elapsed = time.time() - start
                print(f"[WARN] Could not overwrite (file locked): {e}", flush=True)
                print(f"Updated rows: {n_updated}", flush=True)
                print(f"Wrote fallback: {fallback}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not apply BIMMS ef_value backfill window fix: {exc}", flush=True)

    # 8e) Post-fix: NEP Switchboards Cat 11 Use of Sold → if Status indicates rolled into NordicEPOD, set co2e (t)=0
    # Must run AFTER window filter and BEFORE rebuilding window totals tables.
    try:
        if window_path is not None:
            import fix_nep_cat11_use_of_sold_co2e_zero as nep_cat11_fix  # type: ignore

            step_name = "fix_nep_cat11_use_of_sold_co2e_zero.apply_fix (in-place window)"
            _print_step(f"Running: {step_name}")
            start = time.time()
            try:
                n_updated = nep_cat11_fix.apply_fix(
                    Path(window_path),
                    Path(window_path),
                    sheet_name="S3 Cat 11 Use of Sold",
                    company_exact="NEP Switchboards",
                    status_exact="NEP SWB emissions rolled into NordicEPOD; set to 0",
                )
                elapsed = time.time() - start
                print(f"Updated rows: {n_updated}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
            except PermissionError as e:
                fallback = Path(window_path).with_name(f"{Path(window_path).stem}_NEP_CAT11_FIX{Path(window_path).suffix}")
                n_updated = nep_cat11_fix.apply_fix(
                    Path(window_path),
                    fallback,
                    sheet_name="S3 Cat 11 Use of Sold",
                    company_exact="NEP Switchboards",
                    status_exact="NEP SWB emissions rolled into NordicEPOD; set to 0",
                )
                window_path = fallback
                elapsed = time.time() - start
                print(f"[WARN] Could not overwrite (file locked): {e}", flush=True)
                print(f"Updated rows: {n_updated}", flush=True)
                print(f"Wrote fallback: {fallback}", flush=True)
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not apply NEP Cat11 window fix: {exc}", flush=True)

        
    # 9) Rebuild GHGP regrouped workbook based on the latest (preferably windowed) output
    try:
        from reorganize_by_ghgp_category import regroup_by_ghgp  # type: ignore

        step_name = "reorganize_by_ghgp_category.regroup_by_ghgp"
        _safe_run(getattr(__import__("reorganize_by_ghgp_category"), "regroup_by_ghgp"), step_name)
    except Exception as exc:
        print(f"[WARN] Could not regroup GHGP workbook after filtering: {exc}", flush=True)

    # 10) Rebuild window totals tables (correct co2e (t) + Spend_Euro) inside the window workbook
    try:
        import Window_total_tables as wtt  # type: ignore

        step_name = "Window_total_tables.main (in-place window totals)"
        _print_step(f"Running: {step_name}")
        start = time.time()
        _ = wtt.main(str(window_path) if window_path is not None else None)
        elapsed = time.time() - start
        print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not rebuild window totals tables: {exc}", flush=True)

    # 11) Forecasting (optional, can be slow)
    if bool(getattr(args, "run_forecasting", False)):
        try:
            import Forecasting as fc  # type: ignore

            step_name = "Forecasting.run_forecasting (to 2030)"
            _print_step(f"Running: {step_name}")
            start = time.time()
            # Forecasting module will always fall back to the newest window workbook under output/
            fc.run_forecasting(str(window_path) if window_path is not None else None, None)
            elapsed = time.time() - start
            print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
        except Exception as exc:
            print(f"[WARN] Could not run forecasting: {exc}", flush=True)
    else:
        print("[info] Skipped Forecasting (default off). Use --run-forecasting to enable.", flush=True)

    # 12) Post-fix: Write dummy_ef_id + dummy_ef_id_name INTO the window workbook.
    # IMPORTANT: This must not affect mapping or totals. It only adds/updates dummy columns and does not touch co2e(t).
    dummy_window_path = None
    try:
        if window_path is not None:
            base_dir = Path(__file__).resolve().parent
            out_dir = STAGE2_OUTPUT_DIR

            # Prefer a deterministic location, but fall back to newest file under output/ (recursive).
            dummy_mapping_csv = out_dir / "dummy_ef_id_mapping_generated.csv"
            if not dummy_mapping_csv.exists():
                try:
                    candidates = [
                        p
                        for p in out_dir.rglob("dummy_ef_id_mapping_generated.csv")
                        if p.is_file() and (not p.name.startswith("~$"))
                    ]
                    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                    dummy_mapping_csv = candidates[0] if candidates else dummy_mapping_csv
                except Exception:
                    pass

            if dummy_mapping_csv.exists():
                import dummy_ef_id_remap as der  # type: ignore

                step_name = "dummy_ef_id_remap.apply_dummy_mapping (in-place window)"
                _print_step(f"Running: {step_name}")
                start = time.time()

                in_path = Path(window_path)
                out_path = in_path

                try:
                    der.apply_dummy_mapping(
                        workbook_in=in_path,
                        workbook_out=out_path,
                        mapping_csv=dummy_mapping_csv,
                        sheet_name="S3 Cat 1 Purchased G&S",
                    )
                    dummy_window_path = out_path
                except PermissionError as e:
                    fallback = in_path.with_name(f"{in_path.stem}_DUMMY_EFID{in_path.suffix}")
                    der.apply_dummy_mapping(
                        workbook_in=in_path,
                        workbook_out=fallback,
                        mapping_csv=dummy_mapping_csv,
                        sheet_name="S3 Cat 1 Purchased G&S",
                    )
                    dummy_window_path = fallback
                    print(f"[WARN] Could not overwrite (file locked): {e}", flush=True)
                    print(f"[info] Wrote dummy remap fallback workbook: {fallback}", flush=True)

                elapsed = time.time() - start
                print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
                print(f"[info] Window workbook with dummy columns: {dummy_window_path}", flush=True)
            else:
                print(f"[info] Skipped dummy remap: mapping CSV not found -> {dummy_mapping_csv}", flush=True)
    except Exception as exc:
        print(f"[WARN] Could not write dummy_ef_id output workbook: {exc}", flush=True)

    # 13) Decarbonization scenarios (user scenarios) – default ON.
    # Uses the dummy output workbook if available; does NOT mutate the mapping outputs.
    if not bool(getattr(args, "skip_decarb_scenarios", False)):
        try:
            import Decarbonization_Scenarios as decarb  # type: ignore

            # Prefer the dummy window workbook; else fall back to window_path (will fail if dummy col missing).
            inp = dummy_window_path if dummy_window_path is not None else (Path(window_path) if window_path is not None else None)
            if inp is None:
                raise RuntimeError("No window workbook path available for decarbonization scenarios.")

            step_name = "Decarbonization_Scenarios.run (--user-scenarios)"
            _print_step(f"Running: {step_name}")
            start = time.time()

            out_path = decarb.run(
                None,
                input_window=str(inp),
                user_scenarios=True,
            )

            elapsed = time.time() - start
            print(f"Finished: {step_name} in {elapsed:0.2f}s", flush=True)
            print(f"[info] Wrote decarb scenarios workbook: {out_path}", flush=True)
        except Exception as exc:
            print(f"[WARN] Could not run decarbonization scenarios: {exc}", flush=True)
    else:
        print("[info] Skipped Decarbonization_Scenarios (requested).", flush=True)

    print("\nAll steps attempted. Please check printed messages for output filenames.", flush=True)


if __name__ == "__main__":
    main()

#6 aydan 12 aya cikarttigimizda araligi Company Totals, Company by GHGP Sheet Totals , GHGP Sheet Totals, Data Type Summary, Company Stacked Data ve Company Stacked Data by Months sheetleri de bu zaman araligina gore degismesi lazim ama. Bunu yapabilir miyiz? Yani ne zaman degistirmek istesem bu araligi ona gore degismeli bu tablolalarda.
# Auditor ile konustugumuzda tablolara baktiginda sirket bazli ve ay bazli contribution oranlarini da gormek istediklerini soylediler. Bunun icin her tablonun yanina contribution oranlarini da yazdirabilir misin? Hem sirket bazli hem ay bazli hem kategori bazli tablolalarimiz var Company Totals, Company by GHGP Sheet Totals , GHGP Sheet Totals, Company Stacked Data ve Company Stacked Data by Months. bu sheetler icin diyorum yani