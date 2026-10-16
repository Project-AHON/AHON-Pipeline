"""Retired legacy earthquake writer; observations use the configured pipeline."""

if __name__ == "__main__":
    raise RuntimeError(
        "Use datasets/philvolcs_earthquake/observation_pipeline.py with explicit "
        "stage, catalog, volume, batch and source period. Legacy tables require "
        "reviewed migration; event Gold awaits an approved identity contract."
    )
