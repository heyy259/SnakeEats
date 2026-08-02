# Contributing Guide

Thanks for your interest in SnakeEats! Here are some guidelines for getting involved.

## Code of Conduct

Please keep communication professional and respectful. Be friendly to newcomers and open to disagreement.

## How to Contribute

### Report a Bug

1. Search the [Issues](https://github.com/heyy259/SnakeEats/issues) page for an existing report of the same problem
2. If there isn't one, open a new issue using the **Bug report** template
3. Clearly describe the reproduction steps, expected behavior, and actual behavior
4. Include your environment details (OS, Python version, SnakeEats commit)

### Suggest a Feature

1. Search the Issues page for a similar suggestion
2. Open a new issue using the **Feature request** template
3. Describe the use case, the approach you'd like, and any alternatives

### Submit Code

1. **Fork** this repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Write your code and tests
4. Make sure tests pass: run `python -m pytest` in each subproject directory
5. Commit your changes: `git commit -m "feat: short description"`
6. Push to your fork: `git push origin feature/my-feature`
7. Open a Pull Request against the `main` branch

### Commit Message Style

Semantic commit messages are recommended:

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation update
- `refactor:` code refactoring
- `test:` test-related changes
- `chore:` build/toolchain updates

## Development Environment

```bash
# Clone the repository
git clone git@github.com:heyy259/SnakeEats.git
cd SnakeEats

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or .venv\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install -e text-stream -e tool-registry
pip install -r stream-editor-py/requirements.txt
```

## Project Structure

```
SnakeEats/
├── text-stream/          # Text stream kernel (WebSocket server + SDK + Web UI)
├── tool-registry/        # Tool registry (tool management + marker scan & execution)
├── stream-editor-py/     # Stream Editor (Flask web app)
├── .github/              # GitHub templates
├── CONTRIBUTING.md       # This file
├── .env.example          # Environment variable template
└── README.md
```

## Testing

Each subproject has its own test suite:

```bash
cd text-stream && python -m pytest
cd ../tool-registry && python -m pytest
cd ../stream-editor-py && python -m pytest
```

Please make sure all tests pass before opening a PR.

## Code Style

- Follow Python PEP 8
- Functions and classes should have clear docstrings
- Error handling: use specific exception types, never a bare `except:`

## Questions

If you have questions about the code structure or design decisions, ask in an issue and we'll get back to you as soon as we can.
