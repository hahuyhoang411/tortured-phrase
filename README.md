# Tortured Phrase Detection Benchmark

A benchmark for evaluating LLM performance in detecting tortured phrases.

## Quick Start

1. Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install dependencies:
```bash
uv sync
```

3. Run the benchmark:
```bash
uv run python scripts/run_benchmark.py
```

## Project Structure

```
├── src/
│   ├── common/        # Shared utilities and LLM interfaces
│   │   ├── llm/       # LLM client abstractions
│   │   └── utils/     # Utility modules
│   └── pipelines/     # Processing pipelines
│       ├── processing/# Data processing and detection
│       └── evaluation/# Evaluation metrics and analysis
├── tests/             # Unit and integration tests
│   ├── unit/          # Unit tests
│   └── integration/   # Integration tests
├── configs/           # Configuration files
├── scripts/           # Benchmark scripts
├── data/              # Input datasets
├── results/           # Benchmark results
└── logs/              # Application logs
```

## License

See LICENSE file for details.
