"""Run repository transformations with an explicit Spark session and job parameters."""

import runpy
import sys
from pathlib import Path

from runtime import parser


class Widgets:
    def __init__(self, values):
        self.values = values

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


def main():
    from types import SimpleNamespace

    from pyspark.sql import SparkSession
    from quality import check, ensure_monitoring

    p = parser(__doc__)
    p.add_argument("--file", required=True)
    p.add_argument("--source-period", default="Q2_2024")
    args = p.parse_args()
    root = Path(__file__).resolve().parents[3]
    path = (root / args.file).resolve()
    if not path.is_relative_to(root) or path.suffix != ".py":
        raise ValueError("Expected a repository Python file")
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")
    ensure_monitoring(spark, args.catalog)
    sys.path.insert(0, str(path.parent))
    sys.argv = [str(path)]
    try:
        runpy.run_path(
            str(path),
            run_name="__main__",
            init_globals={
                "spark": spark,
                "dbutils": SimpleNamespace(widgets=Widgets(vars(args))),
                "display": lambda frame: frame.show(25, truncate=False),
                "PIPELINE_CATALOG": args.catalog,
                "PIPELINE_PERIOD": args.source_period,
                "PIPELINE_RUN_ID": args.batch_id,
            },
        )
    except Exception:
        check(spark, args.catalog, args.batch_id, "python_transformation", args.file, 1)
        raise


if __name__ == "__main__":
    main()
